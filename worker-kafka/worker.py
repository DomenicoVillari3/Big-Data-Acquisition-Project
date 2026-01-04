import json
import logging
from kafka import KafkaConsumer, KafkaProducer
import geopandas as gpd
import stackstac
import pystac_client
import rasterio
from rasterio.windows import Window
import numpy as np
import os
from geocube.api.core import make_geocube
import time
import random
import io
import boto3
import sys
import argparse
import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="SatelliteDataWorkerBalanced - CROP vs NON-CROP worker"
    )
    parser.add_argument(
        "--worker-id",
        type=int,
        default=random.randint(1, 9999),
        help="ID univoco del worker (es. 'w1', 'nodeA-1'). Se non specificato, usa un randint."
    )
    

    return parser.parse_args()




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
MINIO_ENDPOINT = "http://192.168.128.236:9000"  # es. "http://192.168.128.50:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "satellite-data"
KAFKA_ADDRESS= "192.168.128.236:9092"
GPKG_PATH="/mnt/mimmo/es_2023_all.gpkg"
CLASSES_MAPPING_PATH="/mnt/mimmo/classes_mapping.json"


class SatelliteDataWorkerBalanced:
    def __init__(self, worker_id, bootstrap_servers=KAFKA_ADDRESS):
        self.worker_id = worker_id
        # Configurazione
        self.gpkg_path = GPKG_PATH
        
        # Consumer per ricevere task
        self.consumer = KafkaConsumer(
            'satellite-tasks',
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='satellite-workers',
            
            # Auto-commit
            enable_auto_commit=False,
            # Timeout
            session_timeout_ms=60000,
            heartbeat_interval_ms=20000,
            max_poll_interval_ms=1800000,
            request_timeout_ms=120000,
            # *** KEEP-ALIVE & RECONNECTION ***
            connections_max_idle_ms=540000,  # 9min (prima scadenza server 10min)
            reconnect_backoff_ms=50,         # Retry veloce
            reconnect_backoff_max_ms=1000,   # Max 1s tra retry
            retry_backoff_ms=100,
            # *** SOCKET OPTIONS per keep-alive TCP ***
            api_version_auto_timeout_ms=3000,
            metadata_max_age_ms=300000,  # Refresh metadata ogni 5min   
        )

        
        # Producer per pubblicare risultati
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
                
        # Bande Sentinel-2 da scaricare
        self.assets = ["blue", "green", "red", "nir", "rededge1",
                      "rededge2", "rededge3", "nir08", "swir16", "swir22"]
        
        # Client MinIO (S3-compatible)
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )

        with open(CLASSES_MAPPING_PATH) as f:
            self.binary_classes = dict(json.load(f))
            f.close()





    def get_sentinel_data(self, bbox):
        """
        Scarica dati Sentinel-2 per il bbox.
        MODIFICATO: Priorità alla stagione vegetativa (Peak Season) per coerenza ML.
        """

        # Strategia: Cerca prima nel periodo di massima vegetazione (Maggio-Giugno in Spagna)
        # Questo massimizza il contrasto tra CROP (Verde) e NON-CROP (Terra/Altro)
        search_windows = [
            # 1. GOLD STANDARD: Tarda primavera (Massimo NDVI per cereali/colture)
            ("peak_spring", "2023-04-15/2023-06-15", 10), 
            
            # 2. SILVER: Allarghiamo a inizio primavera (colture precoci)
            ("early_spring", "2023-03-01/2023-04-14", 20),
            
            # 3. BRONZE: Inizio estate (colture irrigate)
            ("early_summer", "2023-06-16/2023-07-30", 20),
            
            # 4. FALLBACK: Tutto l'anno (solo se disperati)
            ("full_year", "2023-01-01/2023-12-31", 30)
        ]

        catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
        
        selected_item = None
        used_season = None

        # Itera sulle finestre temporali in ordine di priorità
        for season_name, date_range, max_cloud in search_windows:
            try:
                search = catalog.search(
                    collections=["sentinel-2-l2a"],
                    bbox=bbox,
                    datetime=date_range,
                    query={"eo:cloud_cover": {"lt": max_cloud}}
                )
                items = search.item_collection()
                
                if len(items) > 0:
                    # Trovato! Prendiamo l'immagine con meno nuvole in assoluto in questo periodo
                    selected_item = min(items, key=lambda x: x.properties['eo:cloud_cover'])
                    used_season = season_name
                    break # Usciamo dal loop, abbiamo l'immagine migliore
            except Exception as e:
                logger.warning(f"Search error for {season_name}: {e}")
                continue

        if selected_item is None:
            logger.warning(f"❌ No valid images found for bbox {bbox} even after fallbacks.")
            return None

        # Log per debug ML (utile per capire se stiamo usando dati buoni)
        logger.info(
            f"  Using Image: {used_season} | Date: {selected_item.datetime.date()} | "
            f"Cloud: {selected_item.properties['eo:cloud_cover']:.1f}%"
        )

        # Download effettivo con StackStac
        try:
            data = stackstac.stack(
                [selected_item],
                assets=self.assets,
                bounds_latlon=bbox,
                resolution=10,
                epsg=32630,
                fill_value=0,
                rescale=False
            )
            
            if data.sizes['time'] == 0:
                return None
            
            # Squeeze time dimension e calcola (download)
            return data.isel(time=0).astype("uint16").compute()

        except Exception as e:
            logger.error(f"Error downloading/computing raster: {e}")
            return None
    
    def save_npz_to_minio(self, img_np, mask_np, task_id, worker_id):
        """
        Salva l'intera patch (bands + mask) come .npz su MinIO.
        img_np: (C, H, W)
        mask_np: (H, W) con valori 0/1
        """
        buffer = io.BytesIO()
        np.savez_compressed(buffer, bands=img_np, mask=mask_np)
        buffer.seek(0)

        object_name = f"patches/task_{task_id}_worker_{worker_id}.npz"
        self.s3_client.upload_fileobj(
            buffer,
            BUCKET_NAME,
            object_name
        )

        s3_path = f"s3a://{BUCKET_NAME}/{object_name}"
        logger.info(f"  Saved patch to MinIO: {s3_path}")
        return s3_path
    
    def save_parquet_to_minio(self, img_np, mask_np, task_id, worker_id):
        """
        Salva tutte le bande originali (dinamico) + label in Parquet.
        img_np.shape = (N_BANDE, 256, 256) dove N_BANDE può essere 10, 12, etc.
        """
        
        # Shape dinamico
        n_bands, h, w = img_np.shape
        n_pixels = h * w
        logger.info(f"💾 Saving {n_bands} bands, {n_pixels} pixels")
        
        # Flatten dinamico: (N_BANDE, H, W) → (n_pixels, N_BANDE)
        flat_features = img_np.reshape(n_pixels, n_bands)  # ✅ DINAMICO!
        flat_labels = mask_np.flatten()
        
        # Crea colonne DINAMICHE per tutte le bande
        df_data = {
            'patch_id': [f"task_{task_id}_worker_{worker_id}"] * n_pixels,
            'pixel_idx': range(n_pixels),
            'label': flat_labels.astype(int)
        }
        
        # Aggiungi TUTTE le bande come colonne
        for band_idx in range(n_bands):
            df_data[f'band_{band_idx}'] = flat_features[:, band_idx]
        
        
        df = pd.DataFrame(df_data)
        
        # Parquet compresso
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression='snappy', index=False)
        buffer.seek(0)
        
        object_name = f"parquet/task_{task_id}_worker_{worker_id}.parquet"
        self.s3_client.upload_fileobj(buffer, BUCKET_NAME, object_name)
        
        s3_path = f"s3a://{BUCKET_NAME}/{object_name}"
        logger.info(f"💾 Saved {df.shape[0]:,} pixels x {n_bands} bands → {s3_path}")
        
        return s3_path

    
    def process_task(self, task):
        """Processa una singola cella (bbox) e salva UNA patch (bands+mask) su MinIO."""
        task_id = task['task_id']
        bbox = tuple(task['bbox'])

        # mapping binario dal master (EuroCrops -> {0,1})
        binary_classes = task.get('binary_classes', self.binary_classes)

        logger.info(f"[Worker {self.worker_id}] Processing task {task_id}: {bbox}")

        result = {
            "task_id": task_id,
            "worker_id": self.worker_id,
            "bbox": bbox,
            "status": "success",
            "s3_path": None,
            "polygon_count": 0,
            "crop_pixels": 0,
            "noncrop_pixels": 0
        }

        try:
            # 1) Leggi poligoni nella bbox
            local_gdf = gpd.read_file(self.gpkg_path, bbox=bbox)

            if len(local_gdf) < 5:
                result["status"] = "skipped_few_polygons"
                return result

            # 2) Mapping diretto EuroCrops -> 0/1
            local_gdf["binary_label"] = local_gdf["EC_hcat_n"].map(self.binary_classes)
            target_polys = local_gdf.dropna(subset=["binary_label"])

            if len(target_polys) == 0:
                result["status"] = "skipped_no_binary_classes"
                return result

            result["polygon_count"] = int(len(target_polys))
            crop_polys = int((target_polys["binary_label"] == 1).sum())
            noncrop_polys = int((target_polys["binary_label"] == 0).sum())

            logger.info(
                f"  Polygons: {len(target_polys)} "
                f"(CROP={crop_polys}, NON-CROP={noncrop_polys})"
            )

            # 3) Scarica Sentinel-2 multibanda per il bbox
            da = self.get_sentinel_data(bbox)
            if da is None:
                result["status"] = "failed_download"
                return result

            # 4) Riproiezione vettoriali su CRS del raster
            if target_polys.crs != da.rio.crs:
                target_polys = target_polys.to_crs(da.rio.crs)

            # 5) Rasterizza la maschera binaria su griglia Sentinel-2
            cube = make_geocube(
                vector_data=target_polys,
                measurements=["binary_label"],
                like=da,
                fill=0
            )

            img_np = da.to_numpy()  # (C, H, W)
            mask_np = cube.binary_label.fillna(0).to_numpy().astype("uint8")  # (H, W)
            
            TARGET_H, TARGET_W = 256, 256
    
            orig_h, orig_w = img_np.shape[1], img_np.shape[2]
            zoom_h = TARGET_H / orig_h
            zoom_w = TARGET_W / orig_w
            
            # Resize TUTTE le bande originali
            n_bands = img_np.shape[0]  # 10, 12, etc.
            img_fixed = np.zeros((n_bands, TARGET_H, TARGET_W), dtype=np.float32)
            
            for c in range(n_bands):
                from scipy.ndimage import zoom
                img_fixed[c] = zoom(img_np[c], (zoom_h, zoom_w), order=1)
            
            # Resize mask
            mask_fixed = zoom(mask_np, (zoom_h, zoom_w), order=0)
            mask_fixed = (mask_fixed > 0.5).astype('uint8')
            
            logger.info(f"✅ SHAPE: {n_bands} bands ({TARGET_H}x{TARGET_W})")
            
            if img_np.ndim != 3:
                result["status"] = "invalid_image_shape"
                return result

            if np.max(mask_np) == 0:
                # nessun pixel marcato come crop/non-crop (tutti 0)
                result["status"] = "failed_empty_mask"
                return result

            # Statistiche pixel
            result["crop_pixels"] = int((mask_np == 1).sum())
            result["noncrop_pixels"] = int((mask_np == 0).sum())

            logger.info(
                f"  Pixel stats: CROP={result['crop_pixels']}, "
                f"NON-CROP={result['noncrop_pixels']}"
            )

            # 6) Salva patch (bands+mask) su MinIO
            #s3_path = self.save_npz_to_minio(img_np, mask_np, task_id, self.worker_id)
            s3_path = self.save_parquet_to_minio(img_np, mask_np, task_id, self.worker_id)
            result["s3_path"] = s3_path

            # 7) Manda messaggio a topic per Spark
            kafka_msg = {
                "task_id": task_id,
                "worker_id": self.worker_id,
                "bbox": bbox,
                "s3_path": s3_path,
                "shape": list(img_np.shape),  # [C,H,W]
                "timestamp": time.time()
            }
            self.producer.send("satellite-images-ready", value=kafka_msg)
            self.producer.flush()

            logger.info(
                f"   Task {task_id} processed and published to 'satellite-images-ready'"
            )

        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            result["status"] = "failed"
            result["error"] = str(e)

        return result
    


    
    def start(self):
        """Avvia il worker"""
        logger.info(f"Worker {self.worker_id} started, waiting for tasks...")
        
        try:
            while True:
                # Poll con timeout
                msg_pack = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in msg_pack.items():
                    for message in messages:
                        task = message.value
                        result = self.process_task(task)
                        
                        # Pubblica risultato
                        self.producer.send('satellite-results', value=result)
                        self.producer.flush()
                        
                        # Commit esplicito
                        self.consumer.commit()
                        
        except KeyboardInterrupt:
            logger.info(f"Worker {self.worker_id} stopping...")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    args = parse_args() 
    worker_id = str(args.worker_id)
   
    intit_time = time.time()
    worker = SatelliteDataWorkerBalanced(worker_id)
    worker.start()
    end=time.time()-intit_time

    with open("/mnt/mimmo/worker_time_final.txt", "a") as f:
        f.write(f"Worker={worker_id} time={end} sec\n")
        f.close()
    
