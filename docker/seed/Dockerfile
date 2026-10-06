FROM python:3.12-slim

WORKDIR /seed

# [avro] brings in the Schema Registry client and AvroSerializer for the Avro topics, [protobuf]
# the ProtobufSerializer for shipment-events-proto. Without the extras,
# seed_data.py's registry imports fail.
#
# grpcio-tools supplies protoc, used below. Confluent's ProtobufSerializer needs a GENERATED
# message class - it will not serialize from a descriptor - so the .proto has to be compiled at
# image-build time. Generating here rather than committing the _pb2.py keeps it from being
# version-locked to whatever protobuf runtime pip resolves.
RUN pip install --no-cache-dir \
      "confluent-kafka[avro,protobuf,schemaregistry]==2.6.1" \
      grpcio-tools

# Compiled before seed_data.py is copied so that editing the seeder does not invalidate this
# layer and force a protoc re-run on every change.
COPY seed/shipment_events.proto .
RUN python -m grpc_tools.protoc -I. --python_out=. shipment_events.proto

COPY seed/seed_data.py .
# seed_avro_edge_cases.py imports seed_data for its connection handling, so both
# files have to be present even though only one runs by default.
COPY seed/seed_avro_edge_cases.py .

CMD ["python", "seed_data.py"]
