package provisioner

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	zfs "github.com/simt2/go-zfs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
)

// Provision creates a PersistentVolume, sets quota and shares it via NFS.
func (p ZFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	path, err := p.createVolume(options)
	if err != nil {
		return nil, err
	}
	log.WithFields(log.Fields{
		"volume": path,
	}).Info("Created volume")

	// See nfs provisioner in github.com/kubernetes-incubator/external-storage for why we annotate this way and if it's still allowed
	annotations := make(map[string]string)
	annotations[annCreatedBy] = createdBy

	var pv *v1.PersistentVolume

	if p.exportNfs {
		pv = &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        options.PVName,
				Labels:      map[string]string{},
				Annotations: annotations,
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: p.reclaimPolicy,
				AccessModes:                   options.PVC.Spec.AccessModes,
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					NFS: &v1.NFSVolumeSource{
						Server:   p.serverHostname,
						Path:     path,
						ReadOnly: false,
					},
				},
			},
		}
	} else {
		pv = &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        options.PVName,
				Labels:      map[string]string{},
				Annotations: annotations,
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: p.reclaimPolicy,
				AccessModes:                   options.PVC.Spec.AccessModes,
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{
						Path: path,
					},
				},
			},
		}
	}

	log.Debug("Returning pv:")
	log.Debug(*pv)

	return pv, nil
}

// createVolume creates a ZFS dataset and returns its mount path
func (p ZFSProvisioner) createVolume(options controller.VolumeOptions) (string, error) {
	zfsPath := p.parent.Name + "/" + options.PVName
	properties := make(map[string]string)

	if p.exportNfs {
		log.Info("Enabling NFS export with options: ", p.shareOptions)
	} else {
		log.Info("Disabling NFS export")
	}
	properties["sharenfs"] = p.shareOptions

	storageRequest := options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	storageRequestBytes := strconv.FormatInt(storageRequest.Value(), 10)
	properties["refquota"] = storageRequestBytes
	properties["refreservation"] = storageRequestBytes

	dataset, err := zfs.CreateFilesystem(zfsPath, properties)
	if err != nil {
		return "", fmt.Errorf("Creating ZFS dataset failed with: %v", err.Error())
	}

	for _, mountOption := range options.MountOptions {
		log.Info("Processing mountOption: " + mountOption)
		if strings.Contains(mountOption, "gid=") {
			split := strings.Split(mountOption, "=")
			gid, err := strconv.Atoi(split[1])
			if err == nil {
				err := os.Chown(dataset.Mountpoint, -1, gid)
				if err == nil {
					err := os.Chmod(dataset.Mountpoint, 0674)
					if err == nil {
						log.Info("Processed: " + mountOption)
					}else{
						log.Error("Unable to chmod: " + dataset.Mountpoint)
						return "", fmt.Errorf("Chmod of mount point " + dataset.Mountpoint + " failed with: %v", err.Error())
					}
				}else{
					log.Error("Unable to chown to gid: " + strconv.Itoa(gid))
					return "", fmt.Errorf("Chown to gid: " + strconv.Itoa(gid) + " failed with: %v", err.Error())
				}
			}else{
				log.Warn("Ignoring unparsable gid: " + split[1])
			}
		}else{
			log.Warn("Ignoring unknown mount option: " + mountOption)
			log.Warn("Current Options are (white space is important): gid=X where X is a GID number")
		}
	}

	return dataset.Mountpoint, nil
}
