import csv
import json
from json import dumps
from kafka import KafkaProducer


def producer():

    kafkaProducer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x, separators=(',', ':')).encode('utf-8'))

    node_attr_dict = {}

    with open('/home/chandima/ubuntu/software/fyp-datasets/dblp.v14-co-author/dblp_nodes.csv') as file:
        reader = csv.DictReader(file, delimiter=";")
        i=0
        for row in reader:
            val = list(row.values())
            data = val[0].split(',')
            node_id = data[0]
            node_attr = ','.join(data[1:])
            node_attr_dict[node_id] = node_attr

    with open('/home/chandima/ubuntu/software/fyp-datasets/dblp.v14-co-author/dblp_edges.csv') as file:
        reader = csv.DictReader(file, delimiter=";")
        i = 0
        for row in reader:
            val = list(row.values())
            data = val[0].split(',')
            source = int(data[0])
            destination = int(data[1])
            timestamp = float(data[2])
            # weight = float(data[3])

            x = {"source": {
                    "id": source,
                    "properties": node_attr_dict[str(source)]
                    },
                 "destination": {
                    "id": destination,
                    "properties": node_attr_dict[str(destination)]
                    },
                 "properties": {
                    "timestamp": timestamp,
                    "weight": 1
                    }
                 }

            if timestamp == 0 or timestamp == 2023 or timestamp == 2024:
                continue

            l = json.dumps(x, separators=(',', ':'))
            if i % 100000 == 0:
                print(i)
                print(x)

            i = i + 1
            kafkaProducer.send(topic='p1', value=x)
            kafkaProducer.flush()

if __name__ == '__main__':
    producer()
