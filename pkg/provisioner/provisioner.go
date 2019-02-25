package provisioner

import (
	"strconv"
)

import "k8s.io/api/core/v1"
import "github.com/prometheus/client_golang/prometheus"
import "github.com/simt2/go-zfs"
import log "github.com/Sirupsen/logrus"

const (
	annCreatedBy     = "kubernetes.io/createdby"
	createdBy        = "zfs-provisioner"
	provisionerIdKey = "zfs-provisioner-id"
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

	identity string //used to identify this provisioner and the volumes it creates
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
func NewZFSProvisioner(parent *zfs.Dataset, shareOptions string, serverHostname string, provisionerId string, reclaimPolicy string, doNfsExport bool) ZFSProvisioner {
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

		identity: provisionerId,

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
