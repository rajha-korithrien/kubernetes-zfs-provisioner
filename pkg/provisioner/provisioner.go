package provisioner

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"reflect"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strconv"
	"time"
)

const (
	annCreatedBy                = "kubernetes.io/createdby"
	createdBy                   = "zfs-provisioner"
	idKey                       = "zfs-provisioner-id"
	totalCountParam             = "zfs-provisioner-total-count"
	lockPortParam               = "zfs-provisioner-lock-port"
	lockConfigMapNamespaceParam = "zfs-provisioner-lock-namespace"
	lockConfigMapNameParam      = "zfs-provisioner-lock-name"
)

// ZFSProvisioner implements the Provisioner interface to create and export ZFS volumes
type ZFSProvisioner struct {
	parent *zfs.Dataset // The parent dataset

	exportNfs      bool   // true we should export the nfs share, false we should not
	shareOptions   string // Additional nfs export options, comma-separated
	serverHostname string // The hostname that should be returned as NFS Server
	reclaimPolicy  v1.PersistentVolumeReclaimPolicy

	persistentVolumeCapacity *prometheus.Desc
	persistentVolumeUsed     *prometheus.Desc

	provisionerHost string //the host running this provisioner
	alphaId         string //used to provide a unique kubernetes configmap key safe id for this provisioner

	client *kubernetes.Clientset //used to allow us to access objects in kubernetes for syncing/locking
}

//Used to get the information needed to contact the other provisioners in order to setup
//a distributed lock. The []string returned is the hostnames of the other provisioners.
func (p ZFSProvisioner) getProvisionerHostnameInfo(configuredCount int, namespace string, name string) ([]string, error) {
	syncMap, err := p.client.CoreV1().ConfigMaps(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		log.Errorf("unable to get config map %v in namespace %v due to: %v", name, namespace, err)
		return nil, errors.New("unable to get lock information configmap: " + name)
	}
	if syncMap.Data == nil {
		syncMap.Data = make(map[string]string)
	}
	syncMap.Data[p.provisionerHost] = time.Now().String()
	syncMap, err = p.client.CoreV1().ConfigMaps(namespace).Update(syncMap)
	if err != nil {
		log.Errorf("Unable to update configMap %v with id: %v due to: %v", name, p.provisionerHost, err)
		return nil, errors.New("unable to update lock information configmap: " + name)
	}
	for len(syncMap.Data) < configuredCount {
		time.Sleep(5 * time.Second)
		syncMap, err = p.client.CoreV1().ConfigMaps(namespace).Get(name, metav1.GetOptions{})
		if err != nil {
			log.Errorf("unable to get config map %v in namespace %v due to: %v", name, namespace, err)
			return nil, errors.New("unable to get lock information configmap: " + name)
		}
		log.Infof("During provision phase lock configMap length: %v waiting until: %v", len(syncMap.Data), configuredCount)
	}
	keys := reflect.ValueOf(syncMap.Data).MapKeys()
	hostnames := make([]string, len(keys))
	for i := 0; i < len(keys); i++ {
		hostnames[i] = keys[i].String()
	}
	return hostnames, nil
}

//Used to get the configured namespace and name of the config map that will hold the hostnames of the provisioners
//that we need to use when negotiating distributed locks. Also gets the configured tcp port to use to do distributed
//lock communication with those provisioners
//the first thing returned is the namespace of the configMap, the second is the name of the configMap and the third
//is the tcp port
func (p ZFSProvisioner) getProvisionerLockConfig(options controller.VolumeOptions) (string, string, int, error) {
	tmp, ok := options.Parameters[lockPortParam]
	if !ok {
		return "", "", -1, errors.New("didn't find parameter " + lockPortParam + " specifying the tcp port to use for lock information sharing")
	}
	lockTcpPort, err := strconv.Atoi(tmp)
	if err != nil {
		return "", "", -1, errors.New("unable to get get a correct tcp port from parameter value: " + tmp)
	}
	namespace, ok := options.Parameters[lockConfigMapNamespaceParam]
	if !ok {
		return "", "", -1, errors.New("didn't find parameter " + lockConfigMapNamespaceParam + " specifying the namespace of the configmap that holds distributed lock hostnames")
	}
	name, ok := options.Parameters[lockConfigMapNameParam]
	if !ok {
		return "", "", -1, errors.New("didn't find parameter " + lockConfigMapNameParam + " specifying the name of the configmap that holds distributed lock hostnames")
	}
	return namespace, name, lockTcpPort, nil
}

//This will look in the given parameters and find the configured number of provisioners that work together
//under a specific provisioner name. It will provide an error when the configuration can't be found or parsed
func (p ZFSProvisioner) getConfiguredProvisionerCount(options controller.VolumeOptions) (int, error) {
	tmp, ok := options.Parameters[totalCountParam]
	if !ok {
		return -1, errors.New("didn't find parameter " + totalCountParam + " specifying number of nodes")
	}
	totalProvisionerCount, err := strconv.Atoi(tmp)
	if err != nil {
		return -1, errors.New("unable to get get a correct provisioner count from parameter value: " + tmp)
	}
	return totalProvisionerCount, nil
}

// Describe implements prometheus.Collector
func (p ZFSProvisioner) Describe(ch chan<- *prometheus.Desc) {
	ch <- p.persistentVolumeCapacity
	ch <- p.persistentVolumeUsed
}

// Collect implements prometheus.Collector
func (p ZFSProvisioner) Collect(ch chan<- prometheus.Metric) {
	children, err := p.parent.Children(1)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Collecting metrics failed")
	}

	for _, child := range children {
		// Skip shapshots
		if child.Type != "filesystem" {
			continue
		}

		capacity, used, err := p.datasetMetrics(child)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Collecting metrics failed")
		} else {
			ch <- *capacity
			ch <- *used
		}
	}
}

// NewZFSProvisioner returns a new ZFSProvisioner
func NewZFSProvisioner(parent *zfs.Dataset, shareOptions string, serverHostname string, provisionerHostName string,
	reclaimPolicy string, doNfsExport bool, alphaId string) ZFSProvisioner {
	var kubernetesReclaimPolicy v1.PersistentVolumeReclaimPolicy

	// Parse reclaim policy
	switch reclaimPolicy {
	case "Delete":
		kubernetesReclaimPolicy = v1.PersistentVolumeReclaimDelete
	case "Retain":
		kubernetesReclaimPolicy = v1.PersistentVolumeReclaimRetain
	}

	if !doNfsExport {
		shareOptions = "off"
	}

	return ZFSProvisioner{
		parent: parent,

		exportNfs:      doNfsExport,
		shareOptions:   shareOptions,
		serverHostname: serverHostname,
		reclaimPolicy:  kubernetesReclaimPolicy,

		provisionerHost: provisionerHostName,
		alphaId:         alphaId,

		persistentVolumeCapacity: prometheus.NewDesc(
			"zfs_provisioner_persistent_volume_capacity",
			"Capacity of a zfs persistent volume.",
			[]string{"persistent_volume"},
			prometheus.Labels{
				"parent":   parent.Name,
				"hostname": serverHostname,
			},
		),
		persistentVolumeUsed: prometheus.NewDesc(
			"zfs_provisioner_persistent_volume_used",
			"Usage of a zfs persistent volume.",
			[]string{"persistent_volume"},
			prometheus.Labels{
				"parent":   parent.Name,
				"hostname": serverHostname,
			},
		),
	}
}

// datasetMetrics returns prometheus metrics for a given ZFS dataset
func (p ZFSProvisioner) datasetMetrics(dataset *zfs.Dataset) (*prometheus.Metric, *prometheus.Metric, error) {
	capacityString, err := dataset.GetProperty("refquota")
	if err != nil {
		return nil, nil, err
	}
	capacityInt, _ := strconv.Atoi(capacityString)

	usedString, err := dataset.GetProperty("usedbydataset")
	if err != nil {
		return nil, nil, err
	}
	usedInt, _ := strconv.Atoi(usedString)

	capacity := prometheus.MustNewConstMetric(p.persistentVolumeCapacity, prometheus.GaugeValue, float64(capacityInt), dataset.Name)
	used := prometheus.MustNewConstMetric(p.persistentVolumeUsed, prometheus.GaugeValue, float64(usedInt), dataset.Name)

	return &capacity, &used, nil
}
