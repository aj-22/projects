## Basics

Verify if port is running
```sh
netstat -plntu
```
## Create Kafka topics


```sh
sudo systemctl start kafka
## kafka-server-start.sh config/kraft/server.properties

kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic orderstopic

kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orderstopic --from-beginning

kafka-console-producer.sh --broker-list localhost:9092 --topic orderstopic

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic orderstopic

```

## Create Kafka Producer

```Py
import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time

KAFKA_TOPIC_NAME_CONS = "orderstopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

if __name__ == "__main__":
    print("Kafka producer application started")
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        value_serializer=lambda x: dumps(x).encode('utf-8'))
    file_path = r"/home/datamaking/workarea/data/orders.csv"
    orders_pd_df = pd.read_csv(file_path)
    print(orders_pd_df.head(1))

    orders_list = orders_pd_df.to_dict(orient="records")

    print(orders_list[0])

    for order in orders_list:
        message = order
        print("Message to be sent:", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

```