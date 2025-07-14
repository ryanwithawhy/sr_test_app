
import itertools
import csv
import json
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
SR_API_KEY = os.getenv("SR_API_KEY")
SR_API_SECRET = os.getenv("SR_API_SECRET")

USER_AVRO_V2_SCHEMA = {
    "type": "record",
    "name": "UserAvro",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "email", "type": "string"}
    ]
}

def generate_records(count, fields):
    records = []
    for x in range(1, count):
        record = {"id": str(x)}
        if fields > 1:
            record["email"] = f"user{x}@email.com"
        records.append(record)
    return records

def get_avro_serializer(schema_registry_conf, schema_str, config):
    client = SchemaRegistryClient(schema_registry_conf)
    serializer = AvroSerializer(
        schema_registry_client=client,
        schema_str=schema_str,
        conf={
            "auto.register.schemas": config["auto_register_schemas"],
            "use.latest.version": config["use_latest_version"],
            "use.schema.id": config["use_schema_id"],
            # "id.compatibility.strict": config["id_compatibility_strict"],
            # "value.subject.name.strategy": config["value_subject_name_strategy"]
        }
    )
    return serializer


def send_records(config, schema_str, records):
    success_count = 0
    error_messages = []
    delivery_errors = []

    sr_conf = {
        'url': SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f"{SR_API_KEY}:{SR_API_SECRET}"
    }
    if config["value_schema_provided"]:
        try:
            serializer = get_avro_serializer(sr_conf, schema_str, config)
        except Exception as e:

            return 0, [str(e)]
    else:
        return 0, ["value.schema is required for Avro serialization"]

    producer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.getenv("KAFKA_CLUSTER_API_KEY"),
        "sasl.password": os.getenv("KAFKA_CLUSTER_API_SECRET"),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer
    }

    try:
        producer = SerializingProducer(producer_conf)
    except Exception as e:
        return 0, [f"Producer creation failed: {e}"]

    def delivery_report(err, msg):
        nonlocal success_count
        print("call me maybe")
        if err is not None:
            delivery_errors.append(str(err))
        else:
            success_count += 1

    for record in records:
        try:
            producer.produce(
                topic="user_avro",
                key=str(record["id"]),
                value=record,
                on_delivery=delivery_report
            )
            producer.poll(0)  # 👈 ensures delivery_report gets processed

        except Exception as e:
            error_messages.append(f"Produce error: {e}")
            break  # Fail fast on produce error

    producer.flush()
    error_messages.extend(delivery_errors)
    return success_count, error_messages

def main():
    results_file = "test_results/schema_registry_test_results_py.csv"
    if os.path.exists(results_file):
        os.remove(results_file)

    records = generate_records(2, 2)

    auto_register_schemas = [True, False]
    use_latest_version = [True, False]
    use_schema_id = [True, False]
    id_compatibility_strict = [True, False]
    subject_strategies = ["TopicNameStrategy", "RecordNameStrategy", "TopicRecordNameStrategy"]
    schema_provided = [True, False]

    combinations = list(itertools.product(
        auto_register_schemas,
        use_latest_version,
        use_schema_id,
        id_compatibility_strict,
        subject_strategies,
        schema_provided
    ))

    fieldnames = [
        "auto_register_schemas",
        "use_latest_version",
        "use_schema_id",
        "id_compatibility_strict",
        "value_subject_name_strategy",
        "value_schema_provided",
        "successful_writes"
    ]

    with open(results_file, "w", newline='') as csvfile:
        fieldnames = [
            "auto_register_schemas",
            "use_latest_version",
            "use_schema_id",
            "id_compatibility_strict",
            "value_subject_name_strategy",
            "value_schema_provided",
            "successful_writes",
            "errors"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for combo in combinations:
            config = {
                "auto_register_schemas": combo[0],
                "use_latest_version": combo[1],
                "use_schema_id": combo[2],
                "id_compatibility_strict": combo[3],
                "value_subject_name_strategy": combo[4],
                "value_schema_provided": combo[5]
            }

            success_count, errors = send_records(config, USER_AVRO_V2_SCHEMA, records)
            writer.writerow({**config, "successful_writes": success_count, "errors": "; ".join(errors)})


if __name__ == "__main__":
    main()
