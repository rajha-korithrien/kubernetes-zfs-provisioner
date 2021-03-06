package provisioner

import (
	"errors"
	"fmt"
	"regexp"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"

	log "github.com/Sirupsen/logrus"
	zfs "github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
)

// Delete removes a given volume from the server
func (p ZFSProvisioner) Delete(volume *v1.PersistentVolume) error {
	ann, ok := volume.Annotations[idKey]
	if !ok {
		return errors.New("identity annotation not found on PV")
	}
	if ann != p.alphaId {
		return &controller.IgnoredError{"identity annotation on PV does not match ours"}
	}
	err := p.deleteVolume(volume)
	if err != nil {
		return err
	}

	if p.exportNfs {
		log.WithFields(log.Fields{
			"volume": volume.Spec.NFS.Path,
		}).Info("Deleted volume")
	} else {
		log.WithFields(log.Fields{
			"volume": volume.Spec.Local.Path,
		}).Info("Deleted volume")
	}
	return nil
}

// deleteVolume deletes a ZFS dataset from the server
func (p ZFSProvisioner) deleteVolume(volume *v1.PersistentVolume) error {
	children, err := p.parent.Children(0)
	if err != nil {
		return fmt.Errorf("Retrieving ZFS dataset for deletion failed with: %v", err.Error())
	}

	var dataset *zfs.Dataset
	for _, child := range children {
		if child.Type != "filesystem" {
			continue
		}

		matched, _ := regexp.MatchString(`.+\/`+volume.Name, child.Name)
		if matched {
			dataset = child
			break
		}
	}
	if dataset == nil {
		err = fmt.Errorf("Volume %v could not be found", &volume)
	}

	if err != nil {
		return fmt.Errorf("Retrieving ZFS dataset for deletion failed with: %v", err.Error())
	}

	err = dataset.Destroy(zfs.DestroyRecursive)
	if err != nil {
		return fmt.Errorf("Deleting ZFS dataset failed with: %v", err.Error())
	}

	return nil
}
