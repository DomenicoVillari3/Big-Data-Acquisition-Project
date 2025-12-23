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
        
       
         # MAPPATURA DIRETTA BINARIA: EuroCrops -> CROP(1) / NON-CROP(0)
        with open("classes_mapping.json") as f:
            self.binary_classes = dict(json.load(f))
        
        logger.info(f"Loaded {len(self.binary_classes)} classes from JSON")

            

        
   
    def generate_tasks_from_spain(self):
        """Genera task per tutta la Spagna """
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
        
        #lista di coordinate da scorrere
        x_ranges = np.arange(start_x, end_x, grid_step)
        y_ranges = np.arange(start_y, end_y, grid_step)
        

        total_cells = len(x_ranges) * len(y_ranges)
        logger.info(f"Grid: {len(x_ranges)}×{len(y_ranges)} = {total_cells} cells")
        
        tasks = []
        task_id = 0

        #statistiche globali 
        total_crop=0
        total_noncrop=0
        
        # Itera sulla griglia
        for i, x in enumerate(x_ranges):
            for j, y in enumerate(y_ranges):
                # Bbox diretto (senza conversione!)
                cell_bbox = (x, y, x + grid_step, y + grid_step)
                
                # LEGGE GPKG con bbox diretto (come codice originale)
                try:
                    local_gdf = gpd.read_file(self.gpkg_path, bbox=cell_bbox)
                    
                    # Salta celle con meno di 3 poligoni dentro 
                    if len(local_gdf) < 3:
                        continue
                    
                    # Mappa classi
                    local_gdf['binary_id'] = local_gdf['EC_hcat_n'].map(
                        self.binary_classes
                    )
                    #i poligoni target sono solo quelli con binary id definito 
                    target_polys = local_gdf.dropna(subset=['binary_id'])
                    
                    if len(target_polys) == 0:
                        continue
                    
                    # Conta per classe
                    binary_counts = target_polys['binary_id'].value_counts().to_dict()
                    crop_count = binary_counts.get(1, 0)
                    noncrop_count = binary_counts.get(0, 0)

                    # Aggiorna statistiche globali
                    total_crop += crop_count
                    total_noncrop += noncrop_count
                    
                    # Log progress ogni 100 task
                    if task_id % 100 == 0 and task_id > 0:
                        ratio = total_crop / max(total_noncrop, 1)
                        logger.info(
                            f"Task {task_id} | "
                            f"CROP: {total_crop:,} | NON-CROP: {total_noncrop:,} | "
                            f"Ratio: {ratio:.1f}:1"
                        )
                    

                    # Crea task
                    task = {
                        'task_id': task_id,
                        'bbox': cell_bbox,
                        'grid_step': grid_step,
                        'polygon_count': len(target_polys),
                        'binary_distribution': {
                            'crop': crop_count,
                            'non_crop': noncrop_count
                        },

                        'binary_classes': self.binary_classes
                    }
                    tasks.append(task)
                    task_id += 1
                    
                except Exception as e:
                    # Solo debug per primi errori
                    if task_id < 3:
                        logger.debug(f"Error reading cell {cell_bbox}: {e}")
                    continue

        # STATISTICHE FINALI
        logger.info("\n" + "="*60)
        logger.info("DATASET STATISTICS")
        logger.info("="*60)
        logger.info(f"Total tasks generated: {len(tasks)} (from {total_cells} cells)")
        logger.info(f"Total polygons: {total_crop + total_noncrop:,}")
        logger.info(f"CROP polygons: {total_crop:,} ({total_crop/(total_crop+total_noncrop)*100:.1f}%)")
        logger.info(f"NON-CROP polygons: {total_noncrop:,} ({total_noncrop/(total_crop+total_noncrop)*100:.1f}%)")
        
        if total_noncrop > 0:
            balance_ratio = total_crop / total_noncrop
            logger.info(f"Imbalance Ratio: {balance_ratio:.2f}:1")
            
            if balance_ratio > 5:
                logger.warning(
                    f"\n  IMBALANCED DATASET!\n"
                    f"Spark recommendation: Use class_weight={{0: {balance_ratio:.1f}, 1: 1.0}}"
                )
        logger.info("="*60 + "\n")
        
        logger.info(f"Total tasks generated: {len(tasks)} from {total_cells} cells")
        return tasks
    



    
    def publish_tasks(self, tasks):
        """Pubblica i task su Kafka"""
        logger.info(f"Publishing {len(tasks)} tasks to Kafka topic {self.topic}...")
        
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
        logger.info("SATELLITE DATA MASTER")
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
