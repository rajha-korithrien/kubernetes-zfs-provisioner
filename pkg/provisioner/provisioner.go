package provisioner

import (
	"encoding/json"
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strconv"
	"strings"
	"time"
)

const (
	annCreatedBy           = "kubernetes.io/createdby"
	createdBy              = "zfs-provisioner"
	idKey                  = "zfs-provisioner-id"
	claimMapNamespaceParam = "zfs-provisioner-claimMap-namespace"
	claimMapNameParam      = "zfs-provisioner-claimMap-name"
	provisionersListingKey = "zfs-provisioners-listing"
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

// declineProvisionRequest is used to remove an entry in the claimMap that indicates this provisioner handled a provision request.
//
// This is useful when the provisioner gets its information added to the map as handling a specific provision, but then
// an error occurs and the provisioner can not actually produce the desired provision result. Things can happen in this order
// because we don't have an explicit lock around which provisioner handles a given provision, just versioning information
// on the configMap we use to hold provision claims.
//
// This function normally doesn't return anything but will return an error if an error occurs.
func (p ZFSProvisioner) declineProvisionRequest(claimMapNamespace string, claimMapName string, pvcName string, timestamp int64) error {
	claimMap, err := p.client.CoreV1().ConfigMaps(claimMapNamespace).Get(claimMapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Provisioner: %v was unable to get claim configMap: %v in namespace: %v due to: %v",
			p.alphaId, claimMapName, claimMapNamespace, err)
		return err
	}
	if claimMap.Data == nil {
		claimMap.Data = make(map[string]string)
	}
	_, ok := claimMap.Data[pvcName]
	if !ok {
		log.Errorf("Provisioner: %v was asked to decline to process claim: %v but didn't find that claim in the claimMap",
			p.alphaId, pvcName)
		return errors.New("Provisioner: " + p.alphaId + " was asked to decline to process claim: " + pvcName +
			" but didn't find that claim in the claimMap")
	}
	claimMap.Data[p.alphaId] = strconv.FormatInt(timestamp, 10)
	delete(claimMap.Data, pvcName)
	claimMap, err = p.client.CoreV1().ConfigMaps(claimMapNamespace).Update(claimMap)
	if err != nil {
		if strings.Contains(err.Error(), "the object has been modified; please apply your changes to the latest version and try again") {
			log.Infof("Provisioner: %v tried to decline claim provision request: %v but the configMap %v has changed... trying again",
				p.alphaId, pvcName, claimMapName)
			return p.declineProvisionRequest(claimMapNamespace, claimMapName, pvcName, timestamp)
		}
		log.Errorf("Provisioner: %v was unable to update configMap: %v for a decline in namespace %v due to: %v", p.alphaId,
			claimMapName, claimMapNamespace, err)
		return err
	}
	log.Infof("Provisioner: %v has removed its previous claim to provision: %v", p.alphaId, pvcName)
	return nil
}

// claimProvisionRequest is used to inform other provisioners that this provisioner is going to handle the given provision request.
//
// Provision requests get handled by exactly 1 provisioner but the cluster is running some number of provisioners for any
// given storageClass. As such we need a way to ensure that only one provisioner actually goes through the work of provisioning
// the volume for a given request. To do this, we use a Kubernetes configMap because the kubernetes api provides an
// Optimistic Concurrency Control mechanism which we can use to ensure only 1 provision will actually handle a provision request.
//
// This function will return true and the timestamp this provisioner last handled a provision request if the provision request was correctly claimed,
// false and -1 if the provision request has already been claimed or this provisioner should not claim it, or
// false and -1 with an error if we can not determine the claim status.
func (p ZFSProvisioner) claimProvisionRequest(claimMapNamespace string, claimMapName string, pvcName string) (bool, int64, error) {
	claimMap, err := p.client.CoreV1().ConfigMaps(claimMapNamespace).Get(claimMapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Provisioner: %v was unable to get claim configMap: %v in namespace: %v due to: %v",
			p.alphaId, claimMapName, claimMapNamespace, err)
		return false, -1, err
	}
	if claimMap.Data == nil {
		claimMap.Data = make(map[string]string)
	}
	if claimedBy, ok := claimMap.Data[pvcName]; ok {
		//the provision request is already in the configMap, we don't need to do anything
		log.Infof("Provisioner: %v has determined that provision request: %v has already been claimed by: %v", p.alphaId, pvcName, claimedBy)
		return false, -1, nil
	}
	rawProvisionerList, ok := claimMap.Data[provisionersListingKey]
	if !ok {
		log.Errorf("Provisioner: %v was unable to get the raw list of active provisioners from configMap: %v in namespace: %v because key: %v was not found",
			p.alphaId, claimMapName, claimMapNamespace, provisionersListingKey)
		return false, -1, errors.New("provisioner: " + p.alphaId + " was unable to get the raw list of active provisioners from configMap: " +
			claimMapName + " in namespace: " + claimMapNamespace + " because key: " + provisionersListingKey + " was not found")
	}
	var provisionerList []string
	err = json.Unmarshal([]byte(rawProvisionerList), &provisionerList)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to decode the raw list of active provisioners: %v due to %v", p.alphaId, rawProvisionerList, err)
		return false, -1, err
	}
	lastProvisioner, lastProvisionTimestamp, err := determineLastProvisioner(claimMap.Data, provisionerList)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to determine which provisioner last handled a provision request due to: %v", p.alphaId, err)
		return false, -1, err
	}
	if lastProvisioner == p.alphaId {
		//this provisioner is the provisioner to last handle a provision, so we should not handle this one
		log.Infof("Provisioner: %v has determined that the last provision was done by: %v at time %v and will not handle current request for: %v",
			p.alphaId, lastProvisioner, time.Unix(lastProvisionTimestamp*1000, 0).String(), pvcName) //lastProvisionTimestamp is in milliseconds since epoch so we turn it into seconds
		return false, -1, nil
	}
	//At this point we think we can handle this provision request, we try to update the configMap and if the update
	//succeeds, we are now responsible for the claim. If the update fails due to the configMap changing (perhaps someone
	//else got the claim, or someone else handled another different claim) call claimProvisionRequest again this recursive
	//call will keep occurring until:
	//a) we get the claim or
	//b) someone else gets the claim
	var nowMills int64 = time.Now().UnixNano() / 1e6 //this gives us a timestamp in milliseconds since epoch
	claimMap.Data[pvcName] = p.alphaId
	claimMap.Data[p.alphaId] = strconv.FormatInt(nowMills, 10) //this should give us a timestamp in milliseconds since epoch
	claimMap, err = p.client.CoreV1().ConfigMaps(claimMapNamespace).Update(claimMap)
	if err != nil {
		if strings.Contains(err.Error(), "the object has been modified; please apply your changes to the latest version and try again") {
			log.Infof("Provisioner: %v tried to claim provision request: %v but the configMap %v has changed... trying again",
				p.alphaId, pvcName, claimMapName)
			return p.claimProvisionRequest(claimMapNamespace, claimMapName, pvcName)
		}
		log.Errorf("Provisioner: %v was unable to update configMap: %v in namespace %v for a claim due to: %v", p.alphaId,
			claimMapName, claimMapNamespace, err)
		return false, -1, err
	}
	log.Infof("Provisioner: %v has claimed provision request for: %v at time: %v", p.alphaId, pvcName, time.Unix(nowMills*1000, 0))
	return true, lastProvisionTimestamp, nil
}

// determineLastProvisioner is used to find the alphaId of the provisioner that last serviced a provision request.
//
// The configMap we use to hold provision requests (the request names are keys) also holds the alphaId of each provisioner (as a key)
// the names of the provisioners and the names of the provision requests are of different formats so there is no worry about
// a collision. Each alphaId should have a unix timestamp (in milliseconds) as its value. This timestamp is when the provisioner with
// the id as the key last handled a provision request.
//
// This function returns the alphaId of the provisioner with the largest timestamp in the map and the timestamp of its provisioning
// or "", -1 and an error if we are unable to determine any such provisioner.
func determineLastProvisioner(claimMap map[string]string, provisioners []string) (string, int64, error) {
	var largestTimestamp int64 = 0
	var lastProvisioner string
	for _, provisioner := range provisioners {
		log.Infof("checking for last time of: %v", provisioner)
		if rawTime, ok := claimMap[provisioner]; ok {
			var err error = nil
			current, err := strconv.ParseInt(rawTime, 10, 64)
			if err != nil {
				return "", -1, errors.New("unable to parse: " + rawTime + " for provisioner entry: " + provisioner)
			}
			if current > largestTimestamp {
				largestTimestamp = current
				lastProvisioner = provisioner
			}
		}
	}
	return lastProvisioner, largestTimestamp, nil
}

// getProvisionMapInfo is used to get the kubernetes namespace and name of the config map used to track what provisions have been made.
//
// provisions get handled by exactly 1 provisioner, and we use a kubernetes configMap to keep track of which provisioner has
// handeled which provision request. In order to use this configMap, we need to know what namepsace the map lives in and
// the name of the map.
//
// The first return value is the namespace, the second value is the name, the third is an error indicating that we were not
// passed proper configuration parameters.
func (p ZFSProvisioner) getClaimMapInfo(options controller.VolumeOptions) (string, string, error) {
	namespace, ok := options.Parameters[claimMapNamespaceParam]
	if !ok {
		return "", "", errors.New("didn't find parameter " + claimMapNamespaceParam + " specifying the namespace that holds the configmap that tracks what provisions have been accomplished")
	}
	name, ok := options.Parameters[claimMapNameParam]
	if !ok {
		return "", "", errors.New("didn't find parameter " + claimMapNameParam + " specifying the names of the configmap that tracks what provisions have been accomplished")
	}
	return namespace, name, nil
}

// updateProvisionerListing is used to ensure that this provisioner alphaId is known in the configMap used to track handled provisions.
func (p ZFSProvisioner) updateProvisionerListing(claimMapNamespace string, claimMapName string) error {
	claimMap, err := p.client.CoreV1().ConfigMaps(claimMapNamespace).Get(claimMapName, metav1.GetOptions{})
	if err != nil {
		log.Errorf("Provisioner: %v was unable to get claim configMap: %v in namespace: %v due to: %v",
			p.alphaId, claimMapName, claimMapNamespace, err)
		return err
	}
	if claimMap.Data == nil {
		claimMap.Data = make(map[string]string)
	}
	var provisionerList []string
	if rawProvisionerList, ok := claimMap.Data[provisionersListingKey]; ok {
		err = json.Unmarshal([]byte(rawProvisionerList), &provisionerList)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to decode the raw list of active provisioners: %v due to %v", p.alphaId, rawProvisionerList, err)
			return err
		}
	} else {
		provisionerList = make([]string, 0)
	}

	if isValueInStringList(p.alphaId, provisionerList) {
		//our id is already in the data being tracked by the map, so we are done
		log.Infof("Provisioner: %v has found it is already in the provisioner listings", p.alphaId)
		return nil
	}

	//we are here when we need to add our alphaId to the list of data tracked by the map
	provisionerList = append(provisionerList, p.alphaId)
	rawBytes, err := json.Marshal(provisionerList)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to encode the raw list of active provisioners: %v due to %v", p.alphaId, provisionerList, err)
		return err
	}
	claimMap.Data[provisionersListingKey] = string(rawBytes)
	claimMap, err = p.client.CoreV1().ConfigMaps(claimMapNamespace).Update(claimMap)
	if err != nil {
		if strings.Contains(err.Error(), "the object has been modified; please apply your changes to the latest version and try again") {
			log.Infof("Provisioner: %v tried to update provisioner listings but the configMap %v has changed... trying again",
				p.alphaId, claimMapName)
			return p.updateProvisionerListing(claimMapNamespace, claimMapName)
		}
		log.Errorf("Provisioner: %v was unable to update configMap: %v in namespace %v for provisioner listing update to: %v", p.alphaId,
			claimMapName, claimMapNamespace, err)
		return err
	}
	log.Infof("Provisioner: %v has completed updating the provisioner listings", p.alphaId)
	return nil
}

func isValueInStringList(value string, list []string) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
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
