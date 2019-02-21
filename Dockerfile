FROM alpine:latest
MAINTAINER Rajha Korithrien <rajha.korithrien@gmail.com>

#This docker file is useful for creating a docker container that is used on a system where
#The host provides the zfs binary via a volume/bind mount.
#We don't package a zfs binary in this image so the host can ensure it provides a userland binary that matches the
#kernel module.

#This container must be run with privlege, and have access to the hosts /dev/zfs device
COPY /bin/zfs-provisioner /zfs-provisioner
ENTRYPOINT ["/zfs-provisioner"]
