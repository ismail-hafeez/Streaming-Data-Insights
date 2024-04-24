from kafka import KafkaConsumer
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
import pandas as pd
import json
from pymongo import MongoClient

# Kafka consumer configuration
bootstrap_servers = ['localhost:9092']
topic = 'test1-topic'
local_file_path = '/home/ismail/kafka/output.txt'

# Create Kafka consumer
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Function to save data to local file
def save_to_MongoDB(data):
    # MongoDB connection details

    uri = 'mongodb://localhost:27017'
    db_name = 'mydatabase'
    collection_name = 'mycollection'

    # Connect to MongoDB
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    def save_data(data):
        try:
            collection.insert_one(data)
            print("Data saved successfully!")
        except Exception as e:
            print("Error saving data:", e)
        finally:
            # Close MongoDB connection
            client.close()
            print("MongoDB connection closed")

    def process_data(data):
        save_data(data)

def applyApriori(data):
    # Flatten the 'also_buy' lists and handle non-iterable elements
    transactions = []
    for sublist in data['also_buy']:
        if isinstance(sublist, list):
            transactions.extend(sublist)
        else:
            transactions.append(sublist)

    # Convert the transactions into a list of lists format
    transactions = [[str(item) for item in sublist] if isinstance(sublist, list) else [str(sublist)] for sublist in transactions]

    # Encode the transactions into a binary format suitable for Apriori
    encoder = TransactionEncoder()
    transactions_encoded = encoder.fit_transform(transactions)

    # Convert the encoded transactions into a DataFrame
    df1 = pd.DataFrame(transactions_encoded, columns=encoder.columns_)

    # Apply the Apriori algorithm to find frequent itemsets
    frequent_itemsets = apriori(df1, min_support=0.001, use_colnames=True)

    return frequent_itemsets

# Main function
if __name__ == "__main__":
    # Consume messages from Kafka topic
    for message in consumer:
        data = message.value
        freq_items = applyApriori(data)
        save_to_MongoDB(freq_items)
        print('Received and saved on Database:', freq_items)
