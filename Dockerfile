FROM bitnami/spark:3.4.1

# Copy GCS Connector ke folder Spark jars
COPY gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/

# Install Python dependencies
RUN pip install python-dotenv google-cloud-storage pandas

# Copy PySpark job ke dalam container
COPY /code/pyspark_local.py /app/code

# Copy user_viewings.csv ke dalam container
COPY /data/user_viewings.csv /app/data

# Copy service account json
COPY service_account.json /app/credentials/service_account.json

WORKDIR /app

# Set python executable
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command bisa diubah di docker-compose
CMD ["bash"]
