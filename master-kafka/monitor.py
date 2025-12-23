# monitor_balanced.py (VERSIONE COMPLETA AGGIORNATA)

import json
from kafka import KafkaConsumer
import logging
from datetime import datetime
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BalancedMonitor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.consumer = KafkaConsumer(
            'satellite-results',
            bootstrap_servers=[bootstrap_servers],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='monitor-group'
        )
        
        # TARGET REALISTICI (aggiornati)
        self.target_samples = {
            1: 15000,  # Ulivi
            2: 15000,  # Vigneti
            3: 15000,  # Agrumi/Frutta (LIMITE - potrebbe non raggiungerlo)
            4: 15000,  # Mandorle/Bosco
            5: 15000,  # Cereali
            6: 15000,  # Leguminose
            7: 10300,   # Ortaggi (RIDOTTO - solo ~10k poligoni totali)
            8: 15000   # Maggese/Suolo
        }
        
        # Target minimi (fallback se non raggiunge il target)
        self.minimum_acceptable = {
            1: 12000, 2: 12000, 3: 8000, 4: 12000,
            5: 12000, 6: 12000, 7: 3000, 8: 12000
        }
        
        # Nomi classi
        self.class_names = {
            1: "Ulivi",
            2: "Vigneti",
            3: "Agrumi/Frutta",
            4: "Mandorle/Bosco",
            5: "Cereali Inv.",
            6: "Leguminose",
            7: "Ortaggi Estivi",
            8: "Maggese/Suolo"
        }
        
        # Statistiche
        self.stats = {
            'total_tasks': 0,
            'total_chips': 0,
            'chips_per_class': defaultdict(int),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'worker_stats': defaultdict(lambda: {'tasks': 0, 'chips': 0})
        }
        self.start_time = datetime.now()
    
    def update_stats(self, result):
        """Aggiorna statistiche"""
        self.stats['total_tasks'] += 1
        self.stats['total_chips'] += result.get('chips_saved', 0)
        
        # Aggiorna contatori per classe
        for class_id, count in result.get('chips_per_class', {}).items():
            self.stats['chips_per_class'][int(class_id)] += count
        
        # Worker stats
        worker_id = result['worker_id']
        self.stats['worker_stats'][worker_id]['tasks'] += 1
        self.stats['worker_stats'][worker_id]['chips'] += result.get('chips_saved', 0)
        
        status = result['status']
        if status == 'success':
            self.stats['success'] += 1
        elif status.startswith('skipped'):
            self.stats['skipped'] += 1
        else:
            self.stats['failed'] += 1
    
    def print_summary(self):
        """Stampa summary con progresso per classe"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        tasks_per_sec = self.stats['total_tasks'] / elapsed if elapsed > 0 else 0
        
        print("\n" + "="*75)
        print(f"📊 DATASET GENERATION PROGRESS (elapsed: {int(elapsed//60)}m {int(elapsed%60)}s)")
        print("="*75)
        print(f"Tasks: {self.stats['success']} ✓ | "
              f"{self.stats['skipped']} skipped | {self.stats['failed']} ✗")
        print(f"Total chips: {self.stats['total_chips']:,} | "
              f"Speed: {tasks_per_sec:.2f} tasks/sec")
        print(f"\n📈 Class-wise Progress:")
        print("-"*75)
        
        all_done = True
        classes_at_limit = []
        
        for class_id in sorted(self.target_samples.keys()):
            current = self.stats['chips_per_class'][class_id]
            target = self.target_samples[class_id]
            min_accept = self.minimum_acceptable[class_id]
            progress = (current / target * 100) if target > 0 else 0
            remaining = max(0, target - current)
            
            # Barra progresso
            bar_length = 25
            filled = min(bar_length, int(bar_length * current / target)) if target > 0 else 0
            bar = "█" * filled + "░" * (bar_length - filled)
            
            # Status
            if current >= target:
                status = "✓ DONE"
                color = ""
            elif current >= min_accept:
                status = f"⚠ {remaining} to target (min OK)"
                color = ""
                all_done = False
                classes_at_limit.append(class_id)
            else:
                status = f"⏳ {remaining} remaining"
                color = ""
                all_done = False
            
            print(f"Class {class_id} ({self.class_names[class_id]:18s}): "
                  f"{bar} {current:6,}/{target:6,} ({progress:5.1f}%) {status}")
        
        # Worker statistics
        if self.stats['worker_stats']:
            print(f"\n👷 Worker Statistics:")
            print("-"*75)
            for worker_id, wstats in sorted(self.stats['worker_stats'].items()):
                print(f"  Worker {worker_id}: {wstats['tasks']:4d} tasks, "
                      f"{wstats['chips']:6,} chips")
        
        print("="*75 + "\n")
        
        return all_done, classes_at_limit
    
    def check_completion_status(self):
        """Verifica lo stato di completamento"""
        classes_done = []
        classes_near_limit = []
        classes_incomplete = []
        
        for class_id, target in self.target_samples.items():
            current = self.stats['chips_per_class'][class_id]
            min_accept = self.minimum_acceptable[class_id]
            
            if current >= target:
                classes_done.append(class_id)
            elif current >= min_accept:
                classes_near_limit.append(class_id)
            else:
                classes_incomplete.append(class_id)
        
        return classes_done, classes_near_limit, classes_incomplete
    
    def start(self):
        """Avvia il monitor"""
        logger.info("🔍 Monitor started, tracking class balance...")
        logger.info(f"Target: {sum(self.target_samples.values()):,} total chips")
        
        try:
            for message in self.consumer:
                result = message.value
                self.update_stats(result)
                
                # Log singolo task (ogni 5)
                if result['task_id'] % 5 == 0:
                    logger.info(
                        f"Task {result['task_id']:4d} | "
                        f"Worker {result['worker_id']:2s} | "
                        f"Chips: {result.get('chips_saved', 0):2d} | "
                        f"Classes: {result.get('chips_per_class', {})}"
                    )
                
                # Summary ogni 20 task
                if self.stats['total_tasks'] % 20 == 0:
                    all_done, at_limit = self.print_summary()
                    
                    # Check completamento
                    done, near, incomplete = self.check_completion_status()
                    
                    if len(done) == 8:  # Tutte le classi complete
                        print("\n🎉 ALL TARGETS REACHED! 🎉")
                        print("Dataset generation complete. You can stop the workers.")
                        self.print_summary()
                        break
                    
                    elif len(incomplete) == 0:  # Tutte almeno al minimo
                        print("\n⚠️  ALL MINIMUM TARGETS REACHED")
                        print(f"Classes at/near limit: {near}")
                        print("Consider stopping if these classes don't improve.")
                        
        except KeyboardInterrupt:
            logger.info("\n🛑 Monitor stopping...")
            done, near, incomplete = self.check_completion_status()
            
            print("\n" + "="*75)
            print("📊 FINAL REPORT")
            print("="*75)
            print(f"✓ Complete: {len(done)} classes - {done}")
            print(f"⚠ At limit: {len(near)} classes - {near}")
            print(f"⏳ Incomplete: {len(incomplete)} classes - {incomplete}")
            
            self.print_summary()
            
        finally:
            self.consumer.close()

if __name__ == "__main__":
    import sys
    broker = sys.argv[1] if len(sys.argv) > 1 else 'localhost:9092'
    monitor = BalancedMonitor(bootstrap_servers=broker)
    monitor.start()
