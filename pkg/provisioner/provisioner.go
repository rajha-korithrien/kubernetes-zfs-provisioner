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
	annCreatedBy               = "kubernetes.io/createdby"
	createdBy                  = "zfs-provisioner"
	idKey                      = "zfs-provisioner-id"
	totalCountParam            = "zfs-provisioner-total-count"
	lockPortParam              = "zfs-provisioner-lock-port"
	lockMapNamespaceParam      = "zfs-provisioner-lock-namespace"
	lockMapNameParam           = "zfs-provisioner-lock-name"
	provisionMapNamespaceParam = "zfs-provisioner-pvc-map-namespace"
	provisionMapNameParam      = "zfs-provisioner-pvc-map-name"
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

	rpcPath string //used to create the rpc end point for communicating distributed mutex operations
}

// getProvisionMapInfo is used to get the kubernetes namespace and name of the config map used to track what provisions have been made.
//
// provisions get handled by exactly 1 provisioner, and we use a kubernetes configMap to keep track of which provisioner has
// handeled which provision request. In order to use this configMap, we need to know what namepsace the map lives in and
// the name of the map.
//
// The first return value is the namespace, the second value is the name, the third is an error indicating that we were not
// passed proper configuration parameters.
func (p ZFSProvisioner) getProvisionMapInfo(options controller.VolumeOptions) (string, string, error) {
	namespace, ok := options.Parameters[provisionMapNamespaceParam]
	if !ok {
		return "", "", errors.New("didn't find parameter " + provisionMapNamespaceParam + " specifying the namespace that holds the configmap that tracks what provisions have been accomplished")
	}
	name, ok := options.Parameters[provisionMapNameParam]
	if !ok {
		return "", "", errors.New("didn't find parameter " + provisionMapNameParam + " specifying the names of the configmap that tracks what provisions have been accomplished")
	}
	return namespace, name, nil
}

// checkAlreadyProvisioned is used to find out if a given provision request has already been handled and if so who handled it.
//
// we use a kubernetes configMap to hold provision request names (as keys) and the provisioner id that handled them (values)
// this method will look for a given pvcName as a key in the configMap and if it is found it will return true and the value
// associated with the given pvcName
func (p ZFSProvisioner) checkAlreadyProvisioned(pvcName string, mapNamespace string, mapName string) (bool, string, error) {
	pvcMap, err := p.client.CoreV1().ConfigMaps(mapNamespace).Get(mapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("unable to get config map %v in namespace %v due to: %v", mapName, mapNamespace, err)
		return false, "", errors.New("unable to get pvc handling information configmap: " + mapName)
	}
	if pvcMap.Data == nil {
		pvcMap.Data = make(map[string]string)
	}
	if handledId, ok := pvcMap.Data[pvcName]; ok {
		return true, handledId, nil
	}
	return false, "", nil
}

// getLastHandledProvisionHandler is used to find out the alphaId of the provisioner that handled the last successfully provisioned provision request.
//
// Each correctly handled provision request places an entry into a kubernetes config map with the key of the provision request name
// and a value of the provisioner. In addition we also place a value at the key "lastHandled" with the alphaId of the provisioner.
// This key gets checked by this function and if it exists the value at that key is returned. If the key does not exist
// then the value "" is returned. We return an error when we can't access the kubernetes configMap
func (p ZFSProvisioner) getLastHandledProvisionHandler(mapNamespace string, mapName string) (string, error) {
	pvcMap, err := p.client.CoreV1().ConfigMaps(mapNamespace).Get(mapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("unable to get config map %v in namespace %v due to: %v", mapName, mapNamespace, err)
		return "", errors.New("unable to get pvc handling information configmap: " + mapName)
	}
	if pvcMap.Data == nil {
		pvcMap.Data = make(map[string]string)
	}
	lastProvisionerId, ok := pvcMap.Data["lastHandled"]
	if !ok {
		return "", nil
	}
	return lastProvisionerId, nil
}

//updateHandledProvisionInfo is used to update the kubernetes configMap that holds what provision requests have been handled.
//
// We place the pvcName in the map as a string and the alphaId of the provisioner as a value such that in the future we can
// check which provisioner handled which provision request.
// We also update the "lastHandled" key to have the value of the alphaId of the current provisioner.
// We only return something on an error.
func (p ZFSProvisioner) updateHandledProvisionInfo(pvcName string, mapNamespace string, mapName string) error {
	pvcMap, err := p.client.CoreV1().ConfigMaps(mapNamespace).Get(mapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("unable to get config map %v in namespace %v due to: %v", mapName, mapNamespace, err)
		return errors.New("unable to get pvc handling information configmap: " + mapName)
	}
	if pvcMap.Data == nil {
		pvcMap.Data = make(map[string]string)
	}
	pvcMap.Data[pvcName] = p.alphaId
	pvcMap.Data["lastHandled"] = p.alphaId
	_, err = p.client.CoreV1().ConfigMaps(mapNamespace).Update(pvcMap)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to update the provision handling map %v in namespace %v due to %v", p.alphaId, mapName, mapNamespace, err)
		return err
	}
	return nil
}

//getLockserversHostnameInfo is used to get the information needed to contact the other provisioners in order to setup a distributed lock.
//
// We use a kubernetes configMap to store the hostnames of all the other provisioners running in the cluster.
// The []string returned is the hostnames of the other provisioners.
func (p ZFSProvisioner) getLockserversHostnameInfo(configuredCount int, namespace string, name string) ([]string, error) {
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
	namespace, ok := options.Parameters[lockMapNamespaceParam]
	if !ok {
		return "", "", -1, errors.New("didn't find parameter " + lockMapNamespaceParam + " specifying the namespace of the configmap that holds distributed lock hostnames")
	}
	name, ok := options.Parameters[lockMapNameParam]
	if !ok {
		return "", "", -1, errors.New("didn't find parameter " + lockMapNameParam + " specifying the name of the configmap that holds distributed lock hostnames")
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
	reclaimPolicy string, doNfsExport bool, alphaId string, kubernetes *kubernetes.Clientset) ZFSProvisioner {
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
		client:          kubernetes,
		rpcPath:         "/lock",

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
