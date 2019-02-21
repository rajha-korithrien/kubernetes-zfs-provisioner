# RancherOS Deployment
RancherOS is a very small linux distro that makes everything into a docker container, including all of its system
services. As such in order to correctly deploy zfs-provisioner to RancherOS as a system service the container must have
access to both the /dev/zfs device and the zfs binary. This poses a problem as when zfs is installed on a RancherOS host
the actual zfs binary (along with zpool) are installed inside a zfs-tools docker image/container. This container does
not export any volumes that we can use so there is no direct way to get ahold of the zfs binary from within another
docker container.

## Workaround
RancherOS when it installs its zfs service creates a shell script in the "console" container that wraps a call around
the execution of the zfs-tools image. We do the same here, the idea being that we run zfs-provisioner as a custom
RancherOS service inside its own container. The container is setup as follows:
* deploy/rancher/zfs - is a shell script that calls the RancherOS 'system-docker' command which passes arguments to the
zfs-tools container
* provisioner-entry.sh - is a shell script that we use as the main entrypoint for our service. This script checks that
we can get at the system-docker command, and that we bindmount /host/dev to /dev. This means that our service needs to be
given a copy of the host /dev tree initially as /host/dev

## Building
Use the instructions provided in the overall repo for building the GO based zfs-provisioner binary. You can then create
the RancherOS docker image by doing the following from the root of the repo.
```bash
docker build -t rajhakorithrien/rancher-zfs-provisioner -f Dockerfile.rancher .
docker push rajhakorithrien/rancher-zfs-provisioner:latest
```
Of course you will want to change your image name and version to suite your deployment environment. These instructions
assume deployment to a pre-existing Dockerhub image repo that you will most likely not have push access to. Feel free to
pull this image, but as it must run with privilege, we suggest building your own.

## Deploy to RancherOS
RancherOS provides a mechanism to deploy custom system services via a docker-compose file. We use this docker-compose
file to setup the environment for zfs-provisioner. We provide an example of such a docker-compose file which should be
placed in /var/lib/rancher/conf as its own file, or put into a cloud-config.yml file.
See [the RancherOS custom services documentation](https://rancher.com/docs/os/v1.2/en/system-services/custom-system-services/)
for more details. 