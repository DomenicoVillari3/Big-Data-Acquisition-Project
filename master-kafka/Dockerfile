FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir "numpy<1.25,>=1.24" kagglehub matplotlib kagglehub matplotlib s3fs boto3 rasterio && \
    apt-get clean && \
    mkdir -p /home/spark/.kaggle/logs && \
    chown -R 185:185 /home/spark

USER 185
WORKDIR /opt/spark/workdir
