package main

import (
	"strconv"
	"errors"
	"os"
	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/simt2/go-zfs"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"kubernetes-zfs-provisioner/pkg/provisioner"
	"net/http"
	"os/exec"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strings"
)

const (
	leasePeriod   = controller.DefaultLeaseDuration
	retryPeriod   = controller.DefaultRetryPeriod
	renewDeadline = controller.DefaultRenewDeadline
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

	// Create the provisioner
	zfsProvisioner := provisioner.NewZFSProvisioner(parent, viper.GetString("share_options"), viper.GetString("server_hostname"), viper.GetString("kube_reclaim_policy"), viper.GetBool("enable_export"))

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
		serverVersion.GitVersion, func(provisionController *controller.ProvisionController) error {
			controller.ExponentialBackOffOnError(false)
			return nil
		}, func(provisionController *controller.ProvisionController) error {
			//The second argument used to be failedRetryThreshold which no longer exists so we try to emulate the behavior
			//via the failed provision and delete thresholds
			controller.FailedDeleteThreshold(2)
			controller.FailedProvisionThreshold(2)
			return nil
		}, func(provisionController *controller.ProvisionController) error {
			controller.LeaseDuration(leasePeriod)
			return nil
		}, func(provisionController *controller.ProvisionController) error {
			controller.RenewDeadline(renewDeadline)
			return nil
		}, func(provisionController *controller.ProvisionController) error {
			controller.RetryPeriod(retryPeriod)
			return nil
		}, func(provisionController *controller.ProvisionController) error {
			leaderElection := false
			log.Info("Leader Election for: " + provisionerName + " " + strconv.FormatBool(leaderElection))
			controller.LeaderElection(leaderElection)
			return nil
		})
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
