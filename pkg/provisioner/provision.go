package provisioner

import (
	"fmt"
	"github.com/minio/dsync"
	"os"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
)

var lockServerStarted = false
var dm *dsync.DRWMutex

// Provision creates a PersistentVolume, sets quota and shares it via NFS.
func (p ZFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {

	if !lockServerStarted {
		configuredCount, err := p.getConfiguredProvisionerCount(options)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to get total provisioner count", p.alphaId)
			return nil, err
		}
		namespace, name, tcpPort, err := p.getProvisionerLockConfig(options)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to get distributed lock config information", p.alphaId)
			return nil, err
		}
		//At this point we need to start our local lock server so it can talk with the other provisioners
		//the other provisioners will be clients to this server
		go StartLockServer(tcpPort)
		lockServerStarted = true

		//now we need to start a client that talks to all the other lock servers
		hostnames, err := p.getProvisionerHostnameInfo(configuredCount, namespace, name)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to get configured lock data config map", p.alphaId)
			return nil, err
		}

		//here we need to do something with hostnames and tcpPorts to create the rpc clients that get sent
		//to the dsync.New call
		ds, err := dsync.New(nil, 0)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to create a dsync object")
			return nil, err
		}
		dm = dsync.NewDRWMutex("provision-list", ds)
	}
	log.Infof("Provisioning request: %v with provisioner: %v", options.PVName, p.alphaId)

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
	annotations[idKey] = p.provisionerHost

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
		return "", fmt.Errorf("creating ZFS dataset failed with: %v", err.Error())
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
					} else {
						log.Error("Unable to chmod: " + dataset.Mountpoint)
						destroyErr := dataset.Destroy(zfs.DestroyDefault)
						if destroyErr != nil {
							return "", fmt.Errorf("chmod of mount point: %v failed with: %v and further cleanup of created dataset filed with: %v", dataset.Mountpoint, err.Error(), destroyErr.Error())
						} else {
							return "", fmt.Errorf("chmod of mount point: %v failed with: %v", dataset.Mountpoint, err.Error())
						}
					}
				} else {
					log.Error("Unable to chown to gid: " + strconv.Itoa(gid))
					destroyErr := dataset.Destroy(zfs.DestroyDefault)
					if destroyErr != nil {
						return "", fmt.Errorf("chown to gid: %v failed with: %v and further cleanup of created dataset failed with: %v", gid, err.Error(), destroyErr.Error())
					} else {
						return "", fmt.Errorf("chown to gid: %v failed with: %v", gid, err.Error())
					}
				}
			} else {
				log.Warn("Ignoring unparsable gid: " + split[1])
			}
		} else {
			log.Warn("Ignoring unknown mount option: " + mountOption)
			log.Warn("Current Options are (white space is important): gid=X where X is a GID number")
		}
	}

	return dataset.Mountpoint, nil
}
