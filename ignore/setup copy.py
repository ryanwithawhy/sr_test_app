# import requests
# import subprocess
# import json
# from confluent_kafka.admin import AdminClient, NewTopic
# import os
# from dotenv import load_dotenv
# from requests.auth import HTTPBasicAuth


# load_dotenv()  # take environment variables

# # ====== CONFIGURATION ======
# BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
# SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
# KAFKA_CLUSTER_API_KEY = os.getenv("KAFKA_CLUSTER_API_KEY")
# KAFKA_CLUSTER_API_SECRET = os.getenv("KAFKA_CLUSTER_API_SECRET")
# SR_API_KEY = os.getenv("SR_API_KEY")
# SR_API_SECRET = os.getenv("SR_API_SECRET")

# TOPICS = ["user_json", "user_avro"]
# SCHEMAS = {
#     "user_json": {
#         "type": "JSON",
#         "schema": {
#             "type": "object",
#             "properties": {
#                 "id": {"type": "string"},
#                 "email": {"type": "string"}
#             },
#             "required": ["id", "email"]
#         }
#     },
#     "user_avro": {
#         "type": "AVRO",
#         "schema": {
#             "type": "record",
#             "name": "UserAvro",
#             "fields": [
#                 {"name": "id", "type": "string"},
#                 {"name": "email", "type": "string"}
#             ]
#         }
#     }
# }
# # ===========================

# def create_topics(admin_client, topics):
#     new_topics = [NewTopic(topic, num_partitions=1, replication_factor=3) for topic in topics]
#     fs = admin_client.create_topics(new_topics)
#     for topic, f in fs.items():
#         try:
#             f.result()
#             print(f"✅ Topic created: {topic}")
#         except Exception as e:
#             if "TopicAlreadyExistsError" in str(e):
#                 print(f"ℹ️  Topic already exists: {topic}")
#             else:
#                 print(f"❌ Failed to create topic {topic}: {e}")

# def register_schema(subject, schema_type, schema):
#     headers = {
#         'Content-Type': 'application/vnd.schemaregistry.v1+json',
#         'Authorization': f'Basic {requests.auth._basic_auth_str(SR_API_KEY, SR_API_SECRET)}'
#     }
#     payload = {
#         "schemaType": schema_type,
#         "schema": json.dumps(schema)
#     }
#     url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}-value/versions"
#     auth = HTTPBasicAuth(SR_API_KEY, SR_API_SECRET)
#     resp = requests.post(url, headers=headers, data=json.dumps(payload), auth=auth)
#     if resp.status_code == 200:
#         print(f"✅ Registered {schema_type} schema for {subject}")
#     else:
#         print(f"❌ Failed to register schema for {subject}: {resp.text}")

# def main():
#     print(BOOTSTRAP_SERVERS)
#     admin_client = AdminClient({
#         'bootstrap.servers': BOOTSTRAP_SERVERS,
#         'security.protocol': 'SASL_SSL',
#         'sasl.mechanism': 'PLAIN',
#         'sasl.username': KAFKA_CLUSTER_API_KEY,
#         'sasl.password': KAFKA_CLUSTER_API_SECRET
#     })    
#     create_topics(admin_client, TOPICS)

#     for subject, info in SCHEMAS.items():
#         register_schema(subject, info["type"], info["schema"])

# if __name__ == "__main__":
#     main()

import requests
import subprocess
import json
from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

# ====== CONFIGURATION ======
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
KAFKA_CLUSTER_API_KEY = os.getenv("KAFKA_CLUSTER_API_KEY")
KAFKA_CLUSTER_API_SECRET = os.getenv("KAFKA_CLUSTER_API_SECRET")
SR_API_KEY = os.getenv("SR_API_KEY")
SR_API_SECRET = os.getenv("SR_API_SECRET")
DEFAULT_COMPATIBILITY = "BACKWARD"


TOPICS = ["user_json", "user_avro"]

# 🔧 Define V1 (only id)
SCHEMAS_V1 = {
    "user_json": {
        "type": "JSON",
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"}
            },
            "required": ["id"]
        }
    },
    "user_avro": {
        "type": "AVRO",
        "schema": {
            "type": "record",
            "name": "UserAvro",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }
    }
}

# 🔧 Define V2 (id + email)
SCHEMAS_V2 = {
    "user_json": {
        "type": "JSON",
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "email": {"type": "string"}
            },
            "required": ["id", "email"]
        }
    },
    "user_avro": {
        "type": "AVRO",
        "schema": {
            "type": "record",
            "name": "UserAvro",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }
    }
}

def set_compatibility(subject, compatibility):
    url = f"{SCHEMA_REGISTRY_URL}/config/{subject}-value"
    headers = {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
    }
    payload = {"compatibility": compatibility}
    resp = requests.put(url, headers=headers, data=json.dumps(payload), auth=HTTPBasicAuth(SR_API_KEY, SR_API_SECRET))
    if resp.status_code == 200:
        print(f"🔧 Set compatibility '{compatibility}' for {subject}")
    else:
        print(f"❌ Failed to set compatibility for {subject}: {resp.text}")


def create_topics(admin_client, topics):
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=3) for topic in topics]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"✅ Topic created: {topic}")
        except Exception as e:
            if "TopicAlreadyExistsError" in str(e):
                print(f"ℹ️  Topic already exists: {topic}")
            else:
                print(f"❌ Failed to create topic {topic}: {e}")

def register_schema(subject, schema_type, schema, version_label):
    headers = {
        'Content-Type': 'application/vnd.schemaregistry.v1+json',
    }
    payload = {
        "schemaType": schema_type,
        "schema": json.dumps(schema)
    }
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}-value/versions"
    resp = requests.post(url, headers=headers, data=json.dumps(payload), auth=HTTPBasicAuth(SR_API_KEY, SR_API_SECRET))
    if resp.status_code == 200:
        version = resp.json().get("id", "?")
        print(f"✅ Registered {schema_type} schema for {subject} (v{version_label}) → version ID: {version}")
    else:
        print(f"❌ Failed to register schema for {subject} (v{version_label}): {resp.text}")


def main():
    admin_client = AdminClient({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': KAFKA_CLUSTER_API_KEY,
        'sasl.password': KAFKA_CLUSTER_API_SECRET
    })    
    create_topics(admin_client, TOPICS)

    # Set compatibility once per subject
    for subject in TOPICS:
        set_compatibility(subject, DEFAULT_COMPATIBILITY)

    # Register v1
    for subject, info in SCHEMAS_V1.items():
        register_schema(subject, info["type"], info["schema"], version_label="1")

    # Register v2
    for subject, info in SCHEMAS_V2.items():
        register_schema(subject, info["type"], info["schema"], version_label="2")

if __name__ == "__main__":
    main()
