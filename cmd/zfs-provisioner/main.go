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
	"net/http"
	"os"
	"os/exec"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strings"
	"time"
)

const (
	leasePeriod          = controller.DefaultLeaseDuration
	retryPeriod          = controller.DefaultRetryPeriod
	renewDeadline        = controller.DefaultRenewDeadline
	provisionerNamespace = "zfs-provisioner"
	provisionerSyncMap   = "zfs-provisioner-config"
	//termLimit     = controller.DefaultTermLimit
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
	viper.SetDefault("provisioner_namespace", provisionerNamespace)
	viper.SetDefault("provisioner_config_map_name", provisionerSyncMap)

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

	//first we need to know the number of nodes in the cluster, we expect to be running 1 provisioner
	//with a given name per cluster node
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		log.Fatalf("Unable to get cluster node list: %v", err)
	}
	nodeCount := len(nodes.Items)
	log.Infof("Cluster is determined to have a node count of: %v", nodeCount)
	//Get ahold of the configMap we are going to use to hold our sync information
	namespace := viper.GetString("provisioner_namespace")
	mapName := viper.GetString("provisioner_config_map_name")
	syncMap, err := clientset.CoreV1().ConfigMaps(namespace).Get(mapName, metav1.GetOptions{})
	if err != nil {
		log.Fatalf("Could not access configMap %v in namespace %v due to %v", mapName, namespace, err)
	}

	alphaId := strings.Replace(provisionerName+"-"+hostname, "/", "-", -1)
	if syncMap.Data == nil {
		syncMap.Data = make(map[string]string)
	}
	syncMap.Data[alphaId] = time.Now().String()
	syncMap, err = clientset.CoreV1().ConfigMaps(namespace).Update(syncMap)
	if err != nil {
		log.Fatalf("Unable to update configMap %v with id: %v due to: %v", mapName, alphaId, err)
	}
	for len(syncMap.Data) < nodeCount {
		time.Sleep(5 * time.Second)
		syncMap, err = clientset.CoreV1().ConfigMaps(namespace).Get(mapName, metav1.GetOptions{})
		if err != nil {
			log.Fatalf("Unable to get configMap during initialization phase: %v", err)
		}
		log.Infof("During initialization phase configMap length: %v waiting until: %v", len(syncMap.Data), nodeCount)
	}

	currentId := 0
	idMap := make(map[string]int)
	for key, _ := range syncMap.Data {
		idMap[key] = currentId
		currentId++
	}

	numericId := idMap[alphaId]
	log.Infof("Completed configMap initialization. AlphaId: %v and numericId: %v with total nodes: %v", alphaId, numericId, len(idMap))

	// Create the provisioner
	zfsProvisioner := provisioner.NewZFSProvisioner(parent, viper.GetString("share_options"), viper.GetString("server_hostname"),
		hostname, viper.GetString("kube_reclaim_policy"), viper.GetBool("enable_export"), numericId, len(idMap))

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
