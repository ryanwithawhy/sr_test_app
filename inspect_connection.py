import os
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

# Load .env from parent dir
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_CLUSTER_API_KEY'),
    'sasl.password': os.getenv('KAFKA_CLUSTER_API_SECRET'),

    # === 3 s timeouts ===
    'request.timeout.ms': 3000,
    'default.api.timeout.ms': 3000,

    # recommended for Confluent Cloud
    'client.dns.lookup': 'use_all_dns_ips'
}

admin = AdminClient(conf)

try:
    md = admin.list_topics(timeout=3)    # blocking call
    print(f"Cluster id: {md.cluster_id}")
    print("Brokers:")
    for b in md.brokers.values():
        print(f"  {b.id}: {b.host}:{b.port}")
    print("Topics:")
    for t in md.topics.values():
        print(f"  {t.topic}  (error={t.error})")
except Exception as e:
    print("❌ Metadata fetch failed:")
    print(e)
