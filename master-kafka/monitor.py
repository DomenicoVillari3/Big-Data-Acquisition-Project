# monitor_binary.py

import json
from kafka import KafkaConsumer
import logging
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinaryMonitor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            'satellite-results',
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='monitor-binary-group'
        )
        
        # Statistiche globali
        self.stats = {
            "total_tasks": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "patches_saved": 0,
            "crop_pixels": 0,
            "noncrop_pixels": 0,
            "worker_stats": defaultdict(lambda: {
                "tasks": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "patches": 0,
                "crop_pixels": 0,
                "noncrop_pixels": 0
            })
        }
        self.start_time = datetime.now()
    
    def update_stats(self, result):
        """Aggiorna statistiche in base al messaggio del worker."""
        self.stats["total_tasks"] += 1
        
        worker_id = result.get("worker_id", "unknown")
        status = result.get("status", "unknown")
        
        self.stats["worker_stats"][worker_id]["tasks"] += 1
        
        if status == "success":
            self.stats["success"] += 1
            self.stats["worker_stats"][worker_id]["success"] += 1

            # Un patch .npz salvata su MinIO per ogni task success
            self.stats["patches_saved"] += 1
            self.stats["worker_stats"][worker_id]["patches"] += 1
            
            crop_pixels = int(result.get("crop_pixels", 0))
            noncrop_pixels = int(result.get("noncrop_pixels", 0))
            
            self.stats["crop_pixels"] += crop_pixels
            self.stats["noncrop_pixels"] += noncrop_pixels
            
            self.stats["worker_stats"][worker_id]["crop_pixels"] += crop_pixels
            self.stats["worker_stats"][worker_id]["noncrop_pixels"] += noncrop_pixels
        
        elif status.startswith("skipped"):
            self.stats["skipped"] += 1
            self.stats["worker_stats"][worker_id]["skipped"] += 1
        else:
            self.stats["failed"] += 1
            self.stats["worker_stats"][worker_id]["failed"] += 1
    
    def print_summary(self):
        """Stampa summary orientata al dataset pixel-wise."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        tasks_per_sec = self.stats["total_tasks"] / elapsed if elapsed > 0 else 0
        
        total_pixels = self.stats["crop_pixels"] + self.stats["noncrop_pixels"]
        crop_ratio = (
            self.stats["crop_pixels"] / total_pixels * 100
            if total_pixels > 0 else 0
        )
        
        print("\n" + "="*80)
        print(f" BINARY DATASET PROGRESS (elapsed: {int(elapsed//60)}m {int(elapsed%60)}s)")
        print("="*80)
        print(f"Tasks: {self.stats['success']} ✓ | "
              f"{self.stats['skipped']} skipped | {self.stats['failed']} ✗ | "
              f"Total: {self.stats['total_tasks']}")
        print(f"Patches saved (.npz): {self.stats['patches_saved']}")
        print(f"Speed: {tasks_per_sec:.2f} tasks/sec")
        
        print("\n Pixel Distribution (CROP vs NON-CROP):")
        print("-"*80)
        print(f"  CROP pixels:     {self.stats['crop_pixels']:,}")
        print(f"  NON-CROP pixels: {self.stats['noncrop_pixels']:,}")
        print(f"  Total pixels:    {total_pixels:,}")
        print(f"  CROP ratio:      {crop_ratio:5.1f}%")
        
        print("\n Worker Statistics:")
        print("-"*80)
        for worker_id, wst in sorted(self.stats["worker_stats"].items()):
            total_w = wst["tasks"]
            if total_w == 0:
                continue
            print(
                f"  Worker {worker_id}: "
                f"{wst['tasks']:4d} tasks | "
                f"{wst['success']:4d} ✓, {wst['skipped']:4d} skipped, {wst['failed']:4d} ✗ | "
                f"patches: {wst['patches']:4d} | "
                f"crop_px: {wst['crop_pixels']:8d}, noncrop_px: {wst['noncrop_pixels']:8d}"
            )
        
        print("="*80 + "\n")
    
    def start(self):
        """Avvia il monitor."""
        logger.info("🔍 Binary monitor started (CROP vs NON-CROP)...")
        
        try:
            for message in self.consumer:
                result = message.value
                # Ci aspettiamo messaggi nel formato:
                # {
                #   "task_id": int,
                #   "worker_id": str/int,
                #   "bbox": [...],
                #   "status": "success"/"skipped_..."/"failed",
                #   "s3_path": "...",
                #   "crop_pixels": int,
                #   "noncrop_pixels": int,
                #   ...
                # }
                
                self.update_stats(result)
                
                task_id = result.get("task_id", -1)
                worker_id = result.get("worker_id", "unknown")
                status = result.get("status", "unknown")
                if task_id % 5 == 0:
                    logger.info(
                        f"Task {task_id:4d} | "
                        f"Worker {str(worker_id):>4s} | "
                        f"Status: {status} | "
                        f"CROP_px: {result.get('crop_pixels', 0)}, "
                        f"NON-CROP_px: {result.get('noncrop_pixels', 0)}"
                    )
                
                # Summary ogni 20 task
                if self.stats["total_tasks"] % 20 == 0:
                    self.print_summary()
        
        except KeyboardInterrupt:
            logger.info("\n Monitor stopping...")
            self.print_summary()
        
        finally:
            self.consumer.close()

if __name__ == "__main__":
    import sys
    broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    monitor = BinaryMonitor(bootstrap_servers=broker)
    monitor.start()
