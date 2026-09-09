FROM python:3.12-slim

WORKDIR /seed

# [avro] brings in the Schema Registry client and AvroSerializer for sensor-readings-avro
# (spec Section 3.3). Without the extra, seed_data.py's registry imports fail.
RUN pip install --no-cache-dir "confluent-kafka[avro,schemaregistry]==2.6.1"

COPY seed/seed_data.py .

CMD ["python", "seed_data.py"]
