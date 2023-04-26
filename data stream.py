from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'airline_data'
eof = "__END_OF_FILE__"

with open('/Users/matt/Downloads/1988.csv', 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    next(csvreader)  # skip header
    for row in csvreader:
        producer.send(topic_name, ','.join(row).encode('utf-8'))
        producer.flush()

    producer.send(topic_name, value=eof.encode('utf-8'))

producer.close()
