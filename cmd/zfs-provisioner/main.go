package main

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/simt2/go-zfs"
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"kubernetes-zfs-provisioner/pkg/provisioner"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strconv"
	"strings"
)

const (
	leasePeriod             = controller.DefaultLeaseDuration
	retryPeriod             = controller.DefaultRetryPeriod
	renewDeadline           = controller.DefaultRenewDeadline
	envNodeComparisonSubnet = "node_comparison_subnet"
)

func main() {
	viper.SetEnvPrefix("zfs")
	viper.AutomaticEnv()

	viper.SetDefault("parent_dataset", "")
	viper.SetDefault("share_options", "rw=@10.0.0.0/8")
	viper.SetDefault("server_hostname", "")
	viper.SetDefault("kube_conf", "kube.conf")
	viper.SetDefault("kube_reclaim_policy", "Delete")
	viper.SetDefault("provisioner_name", "gentics.com/zfs")
	viper.SetDefault("metrics_port", "8080")
	viper.SetDefault("debug", false)
	viper.SetDefault("enable_export", true)
	viper.SetDefault("create_unique_name", false)
	viper.SetDefault(envNodeComparisonSubnet, "192.168.20")

	if viper.GetBool("debug") == true {
		log.SetLevel(log.DebugLevel)
		log.Debug("enable_export: ", viper.GetBool("enable_export"))
		log.Debug("create_unique_name: ", viper.GetBool("create_unique_name"))
	}

	var provisionerName = viper.GetString("provisioner_name")
	if (!viper.GetBool("enable_export")) && viper.GetBool("create_unique_name") {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal("NFS export has been disabled, which forces zfs-provisioner to be local only." +
				"The variable create_unique_name has been set so zfs-provisioner must have a unique name, " +
				"the hostname is normally used, but the hostname can not be determined.")
		}
		provisionerName += "-" + hostname
	}

	// Ensure provisioner name is valid
	if errs := validateProvisionerName(provisionerName, field.NewPath("provisioner")); len(errs) != 0 {
		log.WithFields(log.Fields{
			"errors": errs,
		}).Fatal("Invalid provisioner name specified: ", provisionerName)
	}

	// Retrieve kubernetes config and connect to server
	config, err := clientcmd.BuildConfigFromFlags("", viper.GetString("kube_conf"))
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to build config")
	}
	log.WithFields(log.Fields{
		"config": viper.GetString("kube_conf"),
	}).Info("Loaded kubernetes config")

	log.Debug("Found export directive: ", viper.GetBool("enable_export"))

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to create client")
	}

	serverVersion, err := clientset.Discovery().ServerVersion()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Failed to get server version")
	}
	log.WithFields(log.Fields{
		"version": serverVersion.GitVersion,
	}).Info("Retrieved server version")

	// Determine hostname if not set
	if viper.GetString("server_hostname") == "" {
		hostname, err := exec.Command("hostname", "-f").Output()
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Determining server hostname via \"hostname -f\" failed")
		}
		viper.Set("server_hostname", hostname)
	}

	// Load ZFS parent dataset
	if viper.GetString("parent_dataset") == "" {
		log.WithFields(log.Fields{
			"error": errors.New("Parent dataset is not set"),
		}).Fatal("Could not open ZFS parent dataset")
	}
	parent, err := zfs.GetDataset(viper.GetString("parent_dataset"))
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Could not open ZFS parent dataset")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Could not determine the host of the provisioner")
	}

	alphaId := strings.Replace(provisionerName+"-"+hostname, "/", "-", -1)

	//Now we find how kubernetes.io/hostname identifies this node
	clusterNodeList, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Warnf("Provisioner: %v was unable to get the list of cluster nodes due to: %v", alphaId, err)
	}
	kubernetesProvidedNodeNames := make([]string, len(clusterNodeList.Items))
	for i, node := range clusterNodeList.Items {
		kubernetesProvidedNodeNames[i] = node.Name
	}

	var foundKubernetesNodeName string
	//first try our hostname as what kubernetes identifies this node by
	for _, kubernetesNodeName := range kubernetesProvidedNodeNames {
		log.Debugf("Comparing hostname: %v with kubernetes name: %v", hostname, kubernetesNodeName)
		if hostname == kubernetesNodeName {
			foundKubernetesNodeName = kubernetesNodeName
		}
	}
	if foundKubernetesNodeName == "" {
		//second try all our ip addresses as what kubernetes identifies this node by
		addresses, err := net.InterfaceAddrs()
		if err != nil && !viper.GetBool("enable_export") {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatalf("Unable to determine ip addresses in use by host: %v and kubernetes does not identify the host in a way we understand", hostname)
		}
		for _, address := range addresses {
			for _, kubernetesNodeName := range kubernetesProvidedNodeNames {
				log.Debugf("Comparing address: %v with kubernetes name: %v", address.String(), kubernetesNodeName)
				if strings.HasPrefix(address.String(), viper.GetString(envNodeComparisonSubnet)) {
					//We are here when the address from the host is within the user configured subnet to match kubernetes names to host addresses
					//go provides an address in the form of 192.160.20.20/24 which is to say the /24 at the end is the subnet
					//specification, kubernetes only provides addresses (when used as host names) as 192.168.20.20
					tokens := strings.Split(address.String(), "/")
					if tokens[0] == kubernetesNodeName {
						foundKubernetesNodeName = tokens[0]
					}
				}
			}
		}
	}

	if foundKubernetesNodeName == "" && !viper.GetBool("enable_export") {
		//we don't know how kubernetes identifies the node running this provisioner so we can't put the correct nodeSelector
		//annotations in and we need those annotations because we are not creating an NFS volume, but a local one
		log.WithFields(log.Fields{
			"error": errors.New("unable to determine kubernetes node name for host: " + hostname + " but need to when enable_export: " + strconv.FormatBool(viper.GetBool("enable_export"))),
		}).Fatalf("unable to determine kubernetes node name for host: %v but need to when enable_export: %v", hostname, viper.GetBool("enable_export"))
	} else if !viper.GetBool("enable_export") {
		log.Infof("Provisioner: %v is identified by kubernetes as: %v", alphaId, foundKubernetesNodeName)
	}

	// Create the provisioner
	zfsProvisioner := provisioner.NewZFSProvisioner(parent, viper.GetString("share_options"), viper.GetString("server_hostname"),
		hostname, viper.GetString("kube_reclaim_policy"), viper.GetBool("enable_export"), alphaId, foundKubernetesNodeName, clientset)

	// Start and export the prometheus collector
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(zfsProvisioner)
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog:      log.StandardLogger(),
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
	http.Handle("/metrics", handler)
	go func() {
		log.WithFields(log.Fields{
			"error": http.ListenAndServe(":"+viper.GetString("metrics_port"), nil),
		}).Error("Prometheus exporter failed")
	}()
	log.Info("Started Prometheus exporter")

	// Start the controller
	pc := controller.NewProvisionController(clientset, viper.GetString("provisioner_name"), zfsProvisioner,
		serverVersion.GitVersion, controller.ExponentialBackOffOnError(false),
		controller.FailedDeleteThreshold(5), controller.FailedProvisionThreshold(5),
		controller.LeaseDuration(leasePeriod), controller.RenewDeadline(renewDeadline), controller.RetryPeriod(retryPeriod),
		controller.LeaderElection(false))
	log.Info("Listening for events via provisioner name: " + provisionerName)
	pc.Run(wait.NeverStop)
}

// validateProvisioner tests if provisioner is a valid qualified name.
// https://github.com/kubernetes/kubernetes/blob/release-1.4/pkg/apis/storage/validation/validation.go
func validateProvisionerName(provisioner string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(provisioner) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, provisioner))
	}
	if len(provisioner) > 0 {
		for _, msg := range validation.IsQualifiedName(strings.ToLower(provisioner)) {
			allErrs = append(allErrs, field.Invalid(fldPath, provisioner, msg))
		}
	}
	return allErrs
}
