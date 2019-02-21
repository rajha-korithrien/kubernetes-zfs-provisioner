#!/bin/sh

#Check that we are on Rancher and can use ros
if [ ! -e /usr/bin/system-docker ]; then
    echo "This image can only be used on RancherOS when it has access to system-docker."
    exit 1
fi

#Make our device tree be in the correct place
mount --rbind /host/dev /dev > /dev/null 2>&1

#Now we execute the zfs-provisioner
echo "Starting zfs-provisioner via $@"
echo "Providing input name: $ZFS_PROVISIONER_NAME"
exec "$@"
