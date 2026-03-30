from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нових топіків
my_name = "vitalii_vasylets"
topic_names = [
    f'{my_name}_athlete_event_results',
    f'{my_name}_athlete_enriched_agg'
]
num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor) for topic_name in topic_names
]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics '{topic_names}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо чи є наші топіки в списку існуючих топіків 
print('Мої топіки:')
for topic in admin_client.list_topics():
    if my_name in topic:
        print(topic)

# Закриття зв'язку з клієнтом
admin_client.close()