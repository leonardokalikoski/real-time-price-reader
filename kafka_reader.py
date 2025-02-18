import pandas as pd
from kafka import KafkaProducer
import json
import time
import yaml


with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_BOOTSTRAP_SERVER = config["kafka"]["bootstrap_server"]
KAFKA_SOURCE = config["kafka"]["source"]

class Kafka:
    def __init__(self):
        self.producer = None

    def serialize_value(self, message):
        json_value = json.dumps(message).encode('utf-8') # transforms object to json in a kafka readable format (utf-8)
        return json_value


    def producer_start(self):
        self.producer = KafkaProducer(
            bootstrap_servers = [KAFKA_BOOTSTRAP_SERVER],
            value_serializer = self.serialize_value)
        
    
    def consume_process_send(self):
        # Reads Raw Data
        data_source = KAFKA_SOURCE
        df = pd.read_csv(data_source)

        # Loop through each row in the DataFrame and send each row as a message
        for index, row in df.iterrows():
            message = {
                'Symbol': row['Symbol'],
                'Price': row['Price'],
                'Date': pd.to_datetime(row['Date']).strftime('%Y-%m-%d %H:%M:%S')
            }
            self.producer.send(KAFKA_TOPIC, message)
            print(f"Sent: {message}")

    
    def run(self):
        '''starts the producer, consumes and process the topics, then sends the data to spark'''
        self.producer_start()

        try:
            while True:
                self.consume_process_send()

                # Wait for a while before reading the data again
                time.sleep(15)

        except KeyboardInterrupt:
            print("Process interrupted by user.")
        finally:
            self.producer.flush()
            self.producer.close()
            print("Kafka producer has been closed.")




def main():
    kafka_instance = Kafka()
    kafka_instance.run()


if __name__ == '__main__':
    main()