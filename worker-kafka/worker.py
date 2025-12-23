# worker_balanced.py (BASATO SU CODICE FUNZIONANTE)
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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SatelliteDataWorkerBalanced:
    def __init__(self, worker_id, bootstrap_servers='192.168.128.236:9092'):
        self.worker_id = worker_id
        
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
        
        # Configurazione
        self.gpkg_path = "/mnt/mimmo/es_2023_all.gpkg"
        self.output_dir = "/mnt/mimmo/output"
        self.chip_size = 256
        
        # Sampling probabilities (verranno aggiornate dal task)
        self.sampling_probs = {
            1: 0.20, 2: 0.80, 3: 1.00, 4: 0.20,
            5: 0.15, 6: 1.00, 7: 1.00, 8: 0.40
        }
        
        # Mappatura classi
        self.target_classes = {
            'olive_plantations': 1,
            'vineyards_wine_vine_rebland_grapes': 2,
            'citrus_plantations': 3, 'fruit_temperate_climate': 3,
            'fruit_subtropical_climate': 3,
            'almond': 4, 'orchards_fruits': 4, 'tree_wood_forest': 4,
            'other_tree_wood_forest': 4,
            'durum_hard_wheat': 5, 'common_soft_wheat': 5, 'barley': 5,
            'oats': 5, 'triticale': 5,
            'broad_beans_horse_beans': 6, 'chickpeas': 6, 'peas': 6,
            'lentils': 6, 'vetches': 6,
            'tomato': 7, 'melons': 7, 'watermelon': 7, 'potatoes': 7,
            'vegetables_fresh': 7,
            'fallow_land_not_crop': 8, 'bare_arable_land': 8
        }
        
        self.assets = ["blue", "green", "red", "nir", "rededge1",
                      "rededge2", "rededge3", "nir08", "swir16", "swir22"]
    
    def get_sentinel_data(self, bbox):
        """Scarica dati Sentinel-2 per il bbox"""

         # Aggiungi periodi stagionali
        seasonal_periods = {
            'winter': ("2023-01-01/2023-03-20", 20),   # (range, max_cloud)
            'spring': ("2023-03-21/2023-06-20", 20),
            'summer': ("2023-06-21/2023-09-22", 20),
            'autumn': ("2023-09-23/2023-12-20", 20)
        }
        season_keys = list(seasonal_periods.keys())
        season = random.choice(season_keys)


        date_range, max_cloud = seasonal_periods[season]


        try:
            catalog = pystac_client.Client.open(
                "https://earth-search.aws.element84.com/v1"
            )
            search = catalog.search(
                collections=["sentinel-2-l2a"],
                bbox=bbox,
                datetime=date_range,
                query={"eo:cloud_cover": {"lt": max_cloud}}
            )
            
            items = search.item_collection()

            if not len(items):
                logger.warning(f"No images for {season}, for bboc {bbox} trying all seasons...")
                
                # Prova tutte le stagioni con cloud cover incrementale
                for fallback_season, (fallback_range, fallback_cloud) in seasonal_periods.items():
                    search = catalog.search(
                        collections=["sentinel-2-l2a"],
                        bbox=bbox,
                        datetime=fallback_range,
                        query={"eo:cloud_cover": {"lt": fallback_cloud + 10}}
                    )
                    items = search.item_collection()
                    if len(items):
                        season = fallback_season
                        logger.debug(f"  Using fallback: {season}")
                        break

                    if not len(items):
                        return None
            
            # Seleziona immagine con meno cloud
            selected_item = min(items, key=lambda x: x.properties['eo:cloud_cover'])
            
            logger.info(
                f"  Season: {season}, Date: {selected_item.datetime.date()}, "
                f"Cloud: {selected_item.properties['eo:cloud_cover']:.1f}%"
            )
            
            data = stackstac.stack(
                [items[0]],
                assets=self.assets,
                bounds_latlon=bbox,
                resolution=10,
                epsg=32630,
                fill_value=0,
                rescale=False
            )
            
            if data.sizes['time'] == 0:
                return None
            
            return data.isel(time=0).astype("uint16").compute()
            
        except Exception as e:
            logger.error(f"Error downloading: {e}")
            return None
    
    def save_chip(self, img_arr, mask_arr, transform, crs, filename):
        """Salva chip e maschera su disco condiviso"""
        os.makedirs(f"{self.output_dir}/images", exist_ok=True)
        os.makedirs(f"{self.output_dir}/masks", exist_ok=True)
        
        with rasterio.open(
            f"{self.output_dir}/images/{filename}.tif", 'w',
            driver='GTiff', height=img_arr.shape[1], width=img_arr.shape[2],
            count=img_arr.shape[0], dtype='uint16', crs=crs, transform=transform
        ) as dst:
            dst.write(img_arr)
        
        with rasterio.open(
            f"{self.output_dir}/masks/{filename}.tif", 'w',
            driver='GTiff', height=mask_arr.shape[0], width=mask_arr.shape[1],
            count=1, dtype='uint8', nodata=0, crs=crs, transform=transform
        ) as dst:
            dst.write(mask_arr, 1)
    
    def process_task(self, task):
        """Processa una singola cella con sampling probabilistico"""
        task_id = task['task_id']
        bbox = tuple(task['bbox'])
        
        # Aggiorna sampling probs se presenti nel task
        if 'sampling_probs' in task:
            self.sampling_probs = {
                int(k): v for k, v in task['sampling_probs'].items()
            }
        
        logger.info(f"Worker {self.worker_id} processing task {task_id}: {bbox}")
        
        result = {
            'task_id': task_id,
            'worker_id': self.worker_id,
            'bbox': bbox,
            'chips_saved': 0,
            'chips_per_class': {},
            'status': 'success',
            # Passa target al monitor
            'target_samples': task.get('target_samples', {}),
            'sampling_probs': task.get('sampling_probs', {})
        }
        
        

        try:
            # Leggi poligoni nella bbox (DIRETTO - come codice funzionante)
            local_gdf = gpd.read_file(self.gpkg_path, bbox=bbox)
            
            if len(local_gdf) < 3:
                result['status'] = 'skipped_few_polygons'
                return result
            
            # Mappa classi
            local_gdf['label_id'] = local_gdf['EC_hcat_n'].map(
                self.target_classes
            )
            local_gdf = local_gdf.dropna(subset=['label_id'])
            
            if len(local_gdf) == 0:
                result['status'] = 'skipped_no_target_classes'
                return result
            
            # *** FILTRO PROBABILISTICO (come generate_full_dataset.py) ***
            probs = local_gdf['label_id'].map(self.sampling_probs).fillna(0)
            local_gdf['save_me'] = probs >= np.random.rand(len(local_gdf))
            target_polys = local_gdf[local_gdf['save_me']]

            # AGGIUNTO
            sampling_ratio = len(target_polys) / len(local_gdf)
            logger.info(
                f"  Sampling: {len(local_gdf)} → {len(target_polys)} polygons "
                f"({sampling_ratio:.1%} kept)"
            )
            
            if len(target_polys) == 0:
                result['status'] = 'skipped_sampling'
                return result
            
            logger.info(f"  After sampling: {len(target_polys)} polygons")
            
            # Scarica dati satellitari
            da = self.get_sentinel_data(bbox)
            if da is None:
                result['status'] = 'failed_download'
                return result
            
            # Riproiezione
            if local_gdf.crs != da.rio.crs:
                local_gdf = local_gdf.to_crs(da.rio.crs)
                target_polys = target_polys.to_crs(da.rio.crs)
            
            # Genera maschera
            cube = make_geocube(
                vector_data=local_gdf,
                measurements=["label_id"],
                like=da,
                fill=0
            )
            
            img_np = da.to_numpy()
            mask_np = cube.label_id.fillna(0).to_numpy().astype("uint8")
            transform = da.rio.transform()
            
            if np.max(mask_np) == 0:
                result['status'] = 'failed_empty_mask'
                return result
            
            # Estrai chip
            chips_saved = 0
            chips_per_class = {}
            
            for _, row in target_polys.iterrows():
                cx, cy = ~transform * (
                    row.geometry.centroid.x,
                    row.geometry.centroid.y
                )
                cx, cy = int(cx), int(cy)
                
                min_x = cx - self.chip_size // 2
                max_x = cx + self.chip_size // 2
                min_y = cy - self.chip_size // 2
                max_y = cy + self.chip_size // 2
                
                if (min_x < 0 or min_y < 0 or
                    max_x > img_np.shape[2] or max_y > img_np.shape[1]):
                    continue
                
                im_c = img_np[:, min_y:max_y, min_x:max_x]
                mk_c = mask_np[min_y:max_y, min_x:max_x]
                
                if (np.mean(im_c) > 0 and
                    mk_c[self.chip_size//2, self.chip_size//2] > 0):
                    
                    class_id = int(mk_c[self.chip_size//2, self.chip_size//2])
                    
                    filename = f"worker{self.worker_id}_task{task_id}_class{class_id}_chip{chips_saved}"
                    self.save_chip(
                        im_c, mk_c,
                        rasterio.windows.transform(
                            Window(min_x, min_y, self.chip_size, self.chip_size),
                            transform
                        ),
                        da.rio.crs,
                        filename
                    )
                    
                    chips_saved += 1
                    chips_per_class[class_id] = chips_per_class.get(class_id, 0) + 1
            
            result['chips_saved'] = chips_saved
            result['chips_per_class'] = chips_per_class
            
            if chips_saved > 0:
                logger.info(
                    f"  ✓ Task {task_id}: {chips_saved} chips | "
                    f"Distribution: {dict(chips_per_class)}"
                )
            
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            result['status'] = 'failed'
            result['error'] = str(e)
        
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
    import sys
    intit_time = time.time()
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    worker = SatelliteDataWorkerBalanced(worker_id)
    worker.start()
    end=time.time()-intit_time

    with open("/mnt/mimmo/worker_time.txt", "a") as f:
        f.write(f"Worker={worker_id} time={end} sec\n")
        f.close()
    
