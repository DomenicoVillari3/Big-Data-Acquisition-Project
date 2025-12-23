# master_balanced.py (VERSIONE CORRETTA - basata su codice funzionante)
import json
from kafka import KafkaProducer
import geopandas as gpd
import logging
import numpy as np
import pyogrio
from shapely.geometry import box

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SatelliteDataMasterBalanced:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = 'satellite-tasks'
        self.gpkg_path = "/export/mimmo/es_2023_all.gpkg"
        
        # Sampling probabilities
        self.sampling_probs = {
            1: 0.20, 2: 0.80, 3: 1.00, 4: 0.20,
            5: 0.15, 6: 1.00, 7: 1.00, 8: 0.40
        }
        
        # Distribuzione reale
        self.polygon_counts = {
            1: 280158, 2: 84052, 3: 43474, 4: 288701,
            5: 380935, 6: 58024, 7: 10331, 8: 125807
        }
        
        self.conversion_rate = 0.35
        self.target_samples = self._calculate_targets()
        
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
    
    def _calculate_targets(self):
        """Calcola target basato su sampling_probs"""
        targets = {}
        logger.info("Calculating target samples:")
        
        for class_id in range(1, 9):
            poly_count = self.polygon_counts[class_id]
            sampling_prob = self.sampling_probs[class_id]
            estimated = int(poly_count * sampling_prob * self.conversion_rate)
            target = min(15000, estimated)
            targets[class_id] = target
            
            logger.info(
                f"  Class {class_id}: {poly_count:,} polys × {sampling_prob:.2f} "
                f"× {self.conversion_rate:.2f} = {estimated:,} → target: {target:,}"
            )
        
        return targets
    
    def generate_tasks_from_spain(self):
        """Genera task per tutta la Spagna - COME CODICE ORIGINALE"""
        logger.info(f"Reading GPKG: {self.gpkg_path}")
        
        # Leggi solo metadata
        gdf_meta = gpd.read_file(self.gpkg_path, rows=1)
        logger.info(f"GPKG CRS: {gdf_meta.crs}")
        
        # Leggi bounds totali
        info = pyogrio.read_info(self.gpkg_path)
        total_bounds = info['total_bounds']
        logger.info(f"Total bounds: {total_bounds}")
        
        # IMPORTANTE: EPSG:4258 usa stesse coordinate di WGS84
        # Quindi usiamo direttamente i bounds senza conversione!
        start_x = total_bounds[0]
        start_y = total_bounds[1]
        end_x = total_bounds[2]
        end_y = total_bounds[3]
        
        # Griglia 0.1° (~5km)
        grid_step = 0.05
        
        x_ranges = np.arange(start_x, end_x, grid_step)
        y_ranges = np.arange(start_y, end_y, grid_step)
        
        total_cells = len(x_ranges) * len(y_ranges)
        logger.info(f"Grid: {len(x_ranges)}×{len(y_ranges)} = {total_cells} cells")
        
        tasks = []
        task_id = 0
        
        # Itera sulla griglia - COME CODICE ORIGINALE
        for i, x in enumerate(x_ranges):
            for j, y in enumerate(y_ranges):
                # Bbox diretto (senza conversione!)
                cell_bbox = (x, y, x + grid_step, y + grid_step)
                
                # LEGGE GPKG con bbox diretto (come codice originale)
                try:
                    local_gdf = gpd.read_file(self.gpkg_path, bbox=cell_bbox)
                    
                    # Salta celle vuote
                    if len(local_gdf) < 3:
                        continue
                    
                    # Mappa classi
                    local_gdf['label_id'] = local_gdf['EC_hcat_n'].map(
                        self.target_classes
                    )
                    target_polys = local_gdf.dropna(subset=['label_id'])
                    
                    if len(target_polys) == 0:
                        continue
                    
                    # Conta per classe
                    class_counts = target_polys['label_id'].value_counts().to_dict()
                    
                    # Log ogni 50 task
                    if task_id % 50 == 0 and task_id > 0:
                        logger.info(f"Generated {task_id} tasks...")
                    
                    # Crea task
                    task = {
                        'task_id': task_id,
                        'bbox': cell_bbox,  # Stesso CRS del GPKG!
                        'grid_step': grid_step,
                        'polygon_count': len(target_polys),
                        'class_distribution': class_counts,
                        'sampling_probs': self.sampling_probs,
                        'target_samples': self.target_samples
                    }
                    tasks.append(task)
                    task_id += 1
                    
                except Exception as e:
                    # Solo debug per primi errori
                    if task_id < 5:
                        logger.debug(f"Error reading cell {cell_bbox}: {e}")
                    continue
        
        logger.info(f"Total tasks generated: {len(tasks)} from {total_cells} cells")
        return tasks
    
    def publish_tasks(self, tasks):
        """Pubblica i task su Kafka"""
        logger.info(f"Publishing {len(tasks)} tasks to Kafka...")
        
        for task in tasks:
            self.producer.send(
                self.topic,
                key=f"task_{task['task_id']}",
                value=task
            )
            
            if task['task_id'] % 100 == 0:
                logger.debug(f"Published task {task['task_id']}")
        
        self.producer.flush()
        logger.info(f"All {len(tasks)} tasks published successfully")
    
    def run(self):
        """Esegue il master"""
        # Mostra target
        logger.info("\n" + "="*60)
        logger.info("TARGET SAMPLES:")
        logger.info("="*60)
        total_target = sum(self.target_samples.values())
        for class_id, target in sorted(self.target_samples.items()):
            prob = self.sampling_probs[class_id]
            logger.info(f"  Class {class_id}: {target:,} (sampling: {prob:.0%})")
        logger.info(f"  TOTAL: {total_target:,} chips")
        logger.info("="*60 + "\n")
        
        # Genera task per tutta la Spagna
        tasks = self.generate_tasks_from_spain()
        
        if len(tasks) == 0:
            logger.error("No tasks generated! Check GPKG path and area coverage")
            return
        
        # Statistiche
        total_polygons = sum(t['polygon_count'] for t in tasks)
        logger.info(f"Total target polygons: {total_polygons:,}")
        
        # Pubblica
        self.publish_tasks(tasks)
        
        self.producer.close()
        logger.info("Master completed - Workers can start processing")

if __name__ == "__main__":
    master = SatelliteDataMasterBalanced()
    master.run()
