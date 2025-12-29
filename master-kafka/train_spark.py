#!/usr/bin/env python3
# crop_ml_pipeline_csv.py - CSV + Plot + Timing!

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import numpy as np
import s3fs
import io
import os
import gc
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
import matplotlib.pyplot as plt
import seaborn as sns
import subprocess
import boto3
from botocore.config import Config 
from typing import Tuple,List
import sys
import socket
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time 
import pandas as pd

# %%

MASTER_IP = "192.168.128.236"  # IP del Server Master
MINIO_BUCKET_NAME="satellite-data"
MINIO_ADDRESS="http://192.168.128.236:9000"
# Configurazione MinIO
MINIO_ENDPOINT = f"http://{MASTER_IP}:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

RES_DIR = "/home/amministratore/Scrivania/Big Data Acquisition/progetto mimmo/Big-Data-Acquisition-Project/master-kafka/results"
os.makedirs(RES_DIR, exist_ok=True)

def get_local_ip(target_ip):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((target_ip, 80))
            return s.getsockname()[0]
        except:
            return "127.0.0.1"

CLIENT_IP = get_local_ip(MASTER_IP)
print(f"📡 Client: {CLIENT_IP} → Master: {MASTER_IP}")




# Trova automaticamente l'IP della TUA macchina (Client PC7)
# che è visibile al Master.
def get_local_ip(target_ip):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            # Non invia dati, serve solo a capire quale interfaccia di rete usare
            s.connect((target_ip, 80)) 
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

CLIENT_IP = get_local_ip(MASTER_IP)
print(f"📡 Il mio IP Client (Driver) è: {CLIENT_IP}")
print(f"🎯 Mi connetto al Master su: {MASTER_IP}")



# -------------------------------------------------------
# 2. Pulizia sessioni precedenti (Anti-Zombie)
# -------------------------------------------------------
try:
    if 'spark' in globals(): spark.stop()
    if SparkContext._active_spark_context: SparkContext._active_spark_context.stop()
    SparkContext._active_spark_context = None
except Exception:
    pass

# -------------------------------------------------------
# 3. Inizializzazione Spark Session
# -------------------------------------------------------
conf = SparkConf()
conf.setAppName("SatStream-Distributed-Client")
conf.setMaster(f"spark://{MASTER_IP}:7077")

# CONFIGURAZIONE RETE FONDAMENTALE
# Il Driver (questo PC) deve essere raggiungibile dai Worker remoti
conf.set("spark.driver.host", CLIENT_IP)        # <--- CORRETTO: Usa l'IP del Client, non del Master
conf.set("spark.driver.bindAddress", "0.0.0.0") # Ascolta su tutte le interfacce locali
conf.set("spark.driver.port", "20000")          # Porta fissa per firewall
conf.set("spark.blockManager.port", "20001")    # Porta fissa per dati

# Configurazione S3/MinIO
conf.set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")


# Timeout e Heartbeat (utili in reti distribuite)
conf.set("spark.network.timeout", "600s")
conf.set("spark.executor.heartbeatInterval", "60s")

print("🚀 Tentativo di connessione al Cluster...")

try:
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    print(f"\n✅ SUCCESS! Connesso al Cluster.")
    print(f"🔗 Master: {spark.sparkContext.master}")
    print(f" Web UI (Locale): http://{CLIENT_IP}:4040")
    print(f" Web UI (Master): http://{MASTER_IP}:8080")
    
    # Test veloce
    t=time.time()
    count = spark.range(100).count()
    t=time.time()-t
    print(f"🧪 Test di calcolo distribuito: {count} righe contate in {t} secondi.")
    
except Exception as e:
    print(f"\n❌ ERRORE CRITICO:\n{e}")
    print("\nSUGGERIMENTO: Controlla che la porta 7077 sul Master sia aperta e che 'spark.driver.host' sia raggiungibile dai Worker.")

print("✅ Spark pronto!")

# %%
# 📦 CARICA + FEATURES


t=time.time()
pixels_df = spark.read.parquet(f"s3a://{MINIO_BUCKET_NAME}/parquet/*.parquet")

features_df = pixels_df.select(
    (col("band_4")/10000.0).alias("red_b4"),
    (col("band_2")/10000.0).alias("nir_b8"),
    (col("band_8")/10000.0).alias("swir_b11"),
    (col("band_1")/10000.0).alias("green_b3"),
    col("label").cast("float")
).withColumn("ndvi", when((col("nir_b8")+col("red_b4"))>0.001, 
                            (col("nir_b8")-col("red_b4"))/(col("nir_b8")+col("red_b4")))
                .otherwise(0.0))
                
'''.withColumn("ndwi", when((col("green_b3")+col("swir_b11"))>0.001, 
                        (col("green_b3")-col("swir_b11"))/(col("green_b3")+col("swir_b11")))
            .otherwise(0.0)) 
.withColumn("ndmi", when((col("nir_b8")+col("swir_b11"))>0.001, 
                        (col("nir_b8")-col("swir_b11"))/(col("nir_b8")+col("swir_b11")))
            .otherwise(0.0))'''

# Conteggio totale
t_load=time.time()-t
print(f"📊 {features_df.count():,} total pixel | Crop pixels: {features_df.filter(col('label')==1.0).count()/features_df.count()*100:.1f}%")

# %%
# 🎯 UNDERSAMPLING + CSV SAMPLE


t=time.time()
crop_df = features_df.filter(col("label") == 1.0).sample(0.1, seed=42)      # ~9M
noncrop_df = features_df.filter(col("label") == 0.0).sample(0.025, seed=42)  # ~9M
n_crop = crop_df.count()
print(f" Bilanciamento: Crop pixels: {n_crop:,} | Non-Crop pixels: {noncrop_df.count():,}")
del features_df


#noncrop_sample = noncrop_df.sample(False, n_crop/noncrop_df.count(), seed=42)
balanced_df = crop_df.union(noncrop_df)

# 📈 CSV: 100K sample per plotting
sample_csv = balanced_df.sample(False, 100000/balanced_df.count(), seed=42)
csv_df = sample_csv.toPandas()

# SALVA CSV
#pandas_df.to_csv("crop_features_balanced.csv", index=False)
#print(f"💾 crop_features_balanced.csv: {len(pandas_df):,} righe")
t_balance =time.time()-t
gc.collect()

# %%
# 🧠 TRAINING CON PROGRESS
features=["red_b4","nir_b8","swir_b11","ndvi"]

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features_vec", handleInvalid="skip")

scaler = StandardScaler(
    inputCol="features_vec",
    outputCol="scaled_features",  # ← Nuova colonna scalata
    withMean=True,
    withStd=True
)

rf = RandomForestClassifier(
    featuresCol="scaled_features",  # ← USA features scalate!
    labelCol="label",
    numTrees=50,
    maxDepth=10,  # ← Aumenta da 8
    subsamplingRate=0.8,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, rf])
train_df, test_df = balanced_df.randomSplit([0.8, 0.2], seed=42)

t=time.time()
model = pipeline.fit(train_df)
t_train=time.time()-t
gc.collect()

# %%
# 📊 VALUTAZIONE

t=time.time()
predictions = model.transform(test_df)

auc = BinaryClassificationEvaluator(labelCol="label").evaluate(predictions)
f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)

print(f"\n🎯 AUC-ROC: {auc:.4f} | F1: {f1:.4f}")

# Confusion Matrix CSV
cm_df = predictions.groupBy("label", "prediction").count().toPandas()
cm_df.to_csv(os.path.join(RES_DIR,"confusion_matrix.csv"), index=False)
t_eval=time.time()-t
gc.collect()

# %%
# 🌿 FEATURE IMPORTANCE CSV
importances = model.stages[-1].featureImportances
imp_values = [float(importances[i]) for i in range(len(features))]  # ← FIX
imp_df = pd.DataFrame({
    "feature": features,
    "importance": imp_values  # ← Numeri puliti!
})
imp_df.to_csv(os.path.join(RES_DIR,"feature_importance.csv"), index=False)
print("\n🌿 Feature Importance:")
print(imp_df)


# %%
# 📊 GRAFICI SINGOLI LEGGIBILI (file separati)
# 1. NDVI Distribution
plt.figure(figsize=(10, 6))
csv_df['label_cat'] = csv_df['label'].map({0: 'Non-Crop', 1: 'Crop'})
sns.histplot(data=csv_df, x="ndvi", hue="label_cat", bins=50, alpha=0.7)
plt.title("NDVI Distribution - Crop vs Non-Crop", fontsize=14)
plt.savefig(os.path.join(RES_DIR,"ndvi_distribution.png"), dpi=300)
plt.close()

# 2. Confusion Matrix
plt.figure(figsize=(8, 6))
cm_pivot = cm_df.pivot(index='label', columns='prediction', values='count')
sns.heatmap(cm_pivot, annot=True, fmt='d', cmap='Blues')
plt.title("Confusion Matrix", fontsize=14)
plt.savefig(os.path.join(RES_DIR,"confusion_matrix.png"), dpi=300)
plt.close()

# 3. Feature Importance
plt.figure(figsize=(10, 6))
sns.barplot(data=imp_df.sort_values('importance'), x="importance", y="feature")
plt.title("Feature Importance", fontsize=14)
plt.savefig(os.path.join(RES_DIR,"feature_importance.png"), dpi=300)
plt.close()

# 4. NDVI vs Red
plt.figure(figsize=(10, 6))
sns.scatterplot(data=csv_df.sample(20000), x="red_b4", y="ndvi", 
                hue="label_cat", alpha=0.6, s=15)
plt.title("NDVI vs Red (B4)", fontsize=14)
plt.savefig(os.path.join(RES_DIR,"ndvi_vs_red.png"), dpi=300)
plt.close()

print("✅ 4 grafici salvati in file separati!")
gc.collect()

# %%
# ⏱️ SUMMARY TEMPI
print("\n⏱️  TEMPI ESECUZIONE:")
print(f"   Caricamento:    {t_load:.1f}s")
print(f"   Bilanciamento:  {t_balance:.1f}s") 
print(f"   Training:       {t_train:.1f}s")
print(f"   Valutazione:    {t_eval:.1f}s")
total_time = t_load + t_balance + t_train + t_eval
print(f"   ❌ TOTALE:      {total_time:.1f}s ({total_time/60:.1f}min)")

# 💾 SALVA TUTTO
print("\n🏁 COMPLETATO!")
print("📁 File generati:")
print("   crop_features_balanced.csv")
print("   confusion_matrix.csv") 
print("   feature_importance.csv")
print("   crop_ml_results.png")
print("💾 Modello su MinIO!")

spark.stop()
