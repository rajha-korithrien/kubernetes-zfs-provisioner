package provisioner

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/minio/dsync"
	"github.com/simt2/go-zfs"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"sigs.k8s.io/sig-storage-lib-external-provisioner/controller"
	"strconv"
	"strings"
	"time"
)

var dsyncMutex *dsync.DRWMutex = nil //this is what we use to ensure

// Provision creates a PersistentVolume, sets quota and shares it via NFS.
func (p ZFSProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {

	if dsyncMutex == nil && p.client != nil {
		var rpcPaths []string // list of rpc paths where lock server are serving, because we have distributed lock servers
		// this will be the same path for all servers
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
		go StartLockServer(tcpPort, p.rpcPath) //this will start a new thread to service the server

		log.Infof("Continuing after asking LockServer to start. configuredCount: %v namespace: %v name: %v", configuredCount, namespace, name)

		//now we need to start a client that talks to all the other lock servers
		hostnames, err := p.getLockserversHostnameInfo(configuredCount, namespace, name)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to get configured lock data config map", p.alphaId)
			return nil, err
		}

		//here we need to use hostnames and tcpPorts to create the rpc clients that get sent
		//to the dsync.New call
		hostnamesPorts := make([]string, len(hostnames))
		for i, _ := range hostnames {
			hostname := hostnames[i]
			hostnamesPorts[i] = fmt.Sprintf("%s:%d", hostname, tcpPort)
		}
		for i := 0; i < len(hostnamesPorts); i++ {
			rpcPaths = append(rpcPaths, p.rpcPath) //each listener is using the same tcp port and rpc path
		}

		log.Infof("Building rpc clients to hosts: %+q", hostnamesPorts)
		log.Infof("Building rpc clients to paths: %+q", rpcPaths)

		// Initialize net/rpc clients for dsync.
		var clnts []dsync.NetLocker
		for i := range hostnamesPorts {
			clnts = append(clnts, newClient(hostnamesPorts[i], rpcPaths[i]))
		}

		//Now that we have our rpcClients setup we send them to dsync
		ds, err := dsync.New(clnts, 0)
		if err != nil {
			log.Errorf("Provisioner: %v was unable to create a dsync object", p.alphaId)
			return nil, err
		}
		//We can now use dsync to get a distributed read/write mutex object
		dsyncMutex = dsync.NewDRWMutex("provision-list", ds)
	}

	//We loop until we get the lock
	for !dsyncMutex.GetLock(p.alphaId, "provision.go", time.Second*5) {
		log.Infof("Provisioner: %v is waiting for lock while processing pvc: %v", p.alphaId, options.PVName)
	}
	//From this point on, we *must* unlock the lock when:
	// 1) an error occurs
	// 2) when we correctly provision something
	// 3) when we determine we should not provision something

	//The overview of what is going to happen next:
	//Now that we have the lock we determine if this provision request has already been serviced or not.
	//If it has been serviced, we do nothing
	// if it has not been serviced, we check to see who serviced the last request
	// if we serviced the last request, we do nothing
	// if we did not service the last request, we service this request
	provisionMapNamespace, provisionMapName, err := p.getProvisionMapInfo(options)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to get handled provisions information", p.alphaId)
		dsyncMutex.Unlock()
		return nil, err
	}
	alreadyProvisioned, _, err := p.checkAlreadyProvisioned(options.PVName, provisionMapNamespace, provisionMapName)
	if alreadyProvisioned {
		dsyncMutex.Unlock()
		return nil, &controller.IgnoredError{"provision has been handled by the time we got to it"}
	}
	lastProvisionerId, err := p.getLastHandledProvisionHandler(provisionMapNamespace, provisionMapName)
	if err != nil {
		log.Errorf("Provisioner: %v was unable to get handled provisions information", p.alphaId)
		dsyncMutex.Unlock()
		return nil, err
	}
	if lastProvisionerId == p.alphaId {
		log.Infof("Provisioner: %v is not handling provision request: %v because it handled the last request", p.alphaId, options.PVName)
		dsyncMutex.Unlock()
		return nil, &controller.IgnoredError{"Will not handle provision request because we handled the last request"}
	}
	//At this point we need to actually handle this provision request
	log.Infof("Provisioning request: %v with provisioner: %v", options.PVName, p.alphaId)

	path, err := p.createVolume(options)
	if err != nil {
		dsyncMutex.Unlock()
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

	//Now that we have a correctly provisioned claim, we need to update the map of handled claims and unlock the lock
	err = p.updateHandledProvisionInfo(options.PVName, provisionMapNamespace, provisionMapName)
	if err != nil && pv != nil {
		log.Errorf("Provisioner: %v correctly provisioned request %v but was unable to update the provision map, so the volume was deleted, and provisioning registered as failed.", p.alphaId, options.PVName)
		delError := p.deleteVolume(pv)
		log.Errorf("Provisioner: %v had a further error while deleting correctly provisioned volume %v which was %v", p.alphaId, options.PVName, delError)
		return nil, err
	}
	log.Debug("Returning pv:")
	log.Debug(*pv)

	//and lastly we unlock on successful provisioning
	dsyncMutex.Unlock()
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
