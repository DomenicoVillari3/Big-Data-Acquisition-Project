#!/bin/bash

MASTER_IP="192.168.128.236"    # <-- CAMBIA QUI! IP DEL MASTER
EXPORT_DIR="/export/mimmo"
MOUNT_POINT="/mnt/mimmo"

echo "[INFO] Installing NFS client..."
sudo apt update -y
sudo apt install -y nfs-common

echo "[INFO] Creating mount point $MOUNT_POINT"
sudo mkdir -p $MOUNT_POINT

echo "[INFO] Mounting NFS share..."
sudo mount $MASTER_IP:$EXPORT_DIR $MOUNT_POINT

# Check
if mountpoint -q $MOUNT_POINT; then
    echo "[SUCCESS] Mounted at $MOUNT_POINT"
else
    echo "[ERROR] Failed to mount $MOUNT_POINT"
    exit 1
fi

echo "[INFO] Adding to /etc/fstab..."
echo "$MASTER_IP:$EXPORT_DIR   $MOUNT_POINT   nfs   defaults   0   0" | sudo tee -a /etc/fstab

echo "[DONE] Worker ready! Shared folder at $MOUNT_POINT"
