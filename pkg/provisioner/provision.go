package provisioner

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strconv"
	"strings"
)

// Provision creates a PersistentVolume, sets quota and shares it via NFS.
func (p ZFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {

	log.Infof("Provisioner: %v has been given a provision request with SelectedNode: %v", p.alphaId, options.SelectedNode.Name)

	count, err := p.getConfiguredProvisionerCount(options)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to provision request: %v due to: %v", p.alphaId, options.PVName, err)
	}

	claimMapNamespace, claimMapName, err := p.getClaimMapInfo(options)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to provision request: %v due to: %v", p.alphaId, options.PVName, err)
		return nil, err
	}

	err = p.waitForConfiguredProvisioners(claimMapNamespace, claimMapName, count)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to provision request: %v due to: %v", p.alphaId, options.PVName, err)
		return nil, err
	}

	err = p.updateProvisionerListing(claimMapNamespace, claimMapName)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to put itself into the claim map: %v due to: %v", p.alphaId, claimMapName, err)
		return nil, err
	}

	gotClaim, previousTimestamp, err := p.claimProvisionRequest(claimMapNamespace, claimMapName, options.PVName)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to provision request: %v due to: %v", p.alphaId, options.PVName, err)
		return nil, err
	}
	if !gotClaim {
		log.Infof("Provisioner: %v is ignoring provision request: %v because it has already been handled or is being handled by a different provisioner", p.alphaId, options.PVName)
		return nil, &controller.IgnoredError{"the provision " + options.PVName + " was handled by a different provisioner"}
	}
	//At this point we need to actually handle this provision request it is critical that we correctly handle errors
	//from this point on. Specifically when an error occurs, we need to remove our entry in the claim map
	log.Infof("Provisioner: %v will handle provision request: %v", p.alphaId, options.PVName)

	path, err := p.createVolume(options)
	if err != nil {
		declineErr := p.declineProvisionRequest(claimMapNamespace, claimMapName, options.PVName, previousTimestamp)
		if declineErr != nil {
			log.Errorf("Provisioner: %v was unable to correctly decline failed provision for: %v administrator "+
				"intervention is needed to remove the key: %v from the configMap %v in namespace %v. Anything that needs this claim "+
				"will not deploy until this administrative action is taken.",
				p.alphaId, options.PVName, options.PVName, claimMapName, claimMapNamespace)
			return nil, err
		}
		return nil, err
	}
	log.WithFields(log.Fields{
		"volume": path,
	}).Info("Created volume")

	// See nfs provisioner in github.com/kubernetes-incubator/external-storage for why we annotate this way and if it's still allowed
	annotations := make(map[string]string)
	annotations[annCreatedBy] = createdBy
	annotations[idKey] = p.alphaId

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
