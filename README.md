
# SatStream-ML: Fase 1 - Data Ingestion Pipeline

## 📋 Panoramica

**Parte 1 del progetto Big Data**: Pipeline distribuita Kafka → Worker → MinIO per generare dataset **pixel-wise CROP vs NON-CROP** da Sentinel-2 + EuroCrops.

**Flusso dati**:

Master → Kafka(satellite-tasks) → Worker → MinIO(.npz patches) → Kafka(satellite-images-ready) → Spark(ML)
                           ↓
                    Monitor(satellite-results)

## 🛠️ Componenti

| File | Ruolo | Input | Output |
|------|-------|--------|--------|
| `master.py` | Genera task bbox | `/export/mimmo/es_2023_all.gpkg` | Kafka `satellite-tasks` |
| `worker.py` | Processa bbox → patch | GPKG + Sentinel-2 API | MinIO `satellite-data/patches/*.npz` + Kafka `satellite-images-ready` |
| `monitor.py` | Monitoraggio real-time | Kafka `satellite-results` | Console + statistiche pixel |
| `classes_mapping.json` | Binary mapping | - | CROP(1) vs NON-CROP(0) |
| `docker-compose.yml` | Stack (Redpanda + MinIO + Spark) | - | Servizi pronti |

## 📦 Prerequisiti

```
NFS condiviso: /mnt/mimmo (output) + /export/mimmo (GPKG)
Redpanda: 192.168.128.236:9092
MinIO: 192.168.128.236:9000 (bucket: satellite-data)
Python 3.10+ + requirements.txt
```

## 🚀 Setup rapido

```
# 1. Installa dipendenze
pip install -r requirements.txt

# 2. Avvia stack (se non già su)
docker-compose up -d redpanda minio spark-master spark-worker-1 spark-worker-2

# 3. SSH su nodo principale del cluster
ssh user@192.168.128.236

# 4. Test pipeline (3 terminali)
```

## 🔄 Esecuzione

### Terminale 1: Master
```
python master.py
```
**Output atteso**:
```
Grid: 180×120 = 21,600 cells
CROP polygons: 1,145,675 (90.1%)
NON-CROP polygons: 125,807 (9.9%)
✓ Published 2,847 tasks successfully
```

### Terminale 2: Worker (multipli)
```
# Worker 1
python worker.py --worker-id 1 --bootstrap-servers 192.168.128.236:9092

# Worker 2 (parallelo)
python worker.py --worker-id 2 --bootstrap-servers 192.168.128.236:9092
```
**Output atteso**:
```
[Worker 1] Processing task 0: (-7.5, 36.0, -7.45, 36.05)
  Polygons: 45 (CROP=42, NON-CROP=3)
  Season: spring, Date: 2023-05-15, Cloud: 12.3%
  Saved patch to MinIO: s3a://satellite-data/patches/task_0_worker_1.npz
```

### Terminale 3: Monitor
```
python monitor.py 192.168.128.236:9092
```
**Output atteso**:
```
📊 BINARY DATASET PROGRESS
Tasks: 15 ✓ | 3 skipped | 0 ✗
Patches saved (.npz): 15
CROP pixels: 1,234,567  NON-CROP pixels: 145,890  CROP ratio: 89.4%
```

## 🔍 Verifica (dal nodo del cluster)

### 1. MinIO (via mc)
```
mc alias set myminio http://192.168.128.236:9000 minioadmin minioadmin
mc ls myminio/satellite-data/patches/ | wc -l  # Conta patch
mc cp myminio/satellite-data/patches/task_0_worker_1.npz ./
```

### 2. Controlla contenuto .npz
```
python3 -c "
import numpy as np
data = np.load('task_0_worker_1.npz')
print('Bands:', data['bands'].shape)
print('Mask:', data['mask'].shape)
print('Crop px:', (data['mask']==1).sum())
"
```

### 3. Log NFS
```
tail -f /mnt/mimmo/patches_inventory.txt
wc -l /mnt/mimmo/worker_time.txt
```

## 📊 Output atteso

```
✅ ~2,800 task processati
✅ ~2,800 patch .npz su MinIO (~5-10GB)
✅ ~1.2M crop pixels, ~140K non-crop pixels
✅ Ratio CROP/NON-CROP: ~9:1 (perfetto per Spark class_weight)
```

## ⚠️ Risoluzione problemi comuni

| Errore | Causa | Soluzione |
|--------|-------|-----------|
| `No images for bbox` | Nessuna scena Sentinel-2 | Aumenta `max_cloud` a 50% |
| `failed_empty_mask` | Nessun poligono mappato | Verifica `classes_mapping.json` |
| `MinIO connection refused` | Endpoint sbagliato | `http://192.168.128.236:9000` |
| `GPKG not found` | NFS non montato | `./setup_master_nfs.sh` + `./mount_worker_nfs.sh` |

## 🔄 Prossimi passi (Fase 2)

```
MinIO patches → Spark Streaming → Pixel-wise classification (LogisticRegression)
  ↓
Kafka results → Accuracy, F1-score, Confusion Matrix
```

**Dataset pronto per Spark MLlib**: ogni `.npz` = `(10_bande, H, W)` + `mask_binaria(H, W)`

---
