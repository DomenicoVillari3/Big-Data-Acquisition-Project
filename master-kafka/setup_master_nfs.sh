#!/bin/bash

# Directory da condividere
EXPORT_DIR="/export/mimmo"

echo "[INFO] Installing NFS server..."
sudo apt update -y
sudo apt install -y nfs-kernel-server

echo "[INFO] Creating export directory: $EXPORT_DIR"
sudo mkdir -p $EXPORT_DIR

echo "[INFO] Setting permissions..."
sudo chown -R $USER:$USER $EXPORT_DIR
sudo chmod -R 755 $EXPORT_DIR

# Esporta verso tutta la rete LAN 192.168.128.0/24
# CAMBIA LA SUBNET SE NECESSARIO
SUBNET="192.168.128.0/24"

echo "[INFO] Configuring /etc/exports..."
echo "$EXPORT_DIR  $SUBNET(rw,sync,no_subtree_check)" | sudo tee -a /etc/exports

echo "[INFO] Restarting NFS service..."
sudo exportfs -ra
sudo systemctl restart nfs-kernel-server

echo "[SUCCESS] NFS Master ready!"
echo "Shared directory: $EXPORT_DIR"
