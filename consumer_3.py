
from kafka import KafkaConsumer
import json
from collections import defaultdict
from pymongo import MongoClient

# Create a Kafka consumer instance
consumer_instance = KafkaConsumer('assignment-topic', bootstrap_servers='localhost:9092', group_id='my-group')

# Define parameters
min_support_threshold = 0.1

# Initialize data structures
itemset_support_count = defaultdict(int)  
total_transactions_count = 0  

# Function to update the support count of itemsets
def update_support_count(transaction_data):
    for item_data in transaction_data:
        # Convert the item to a hashable format
        item_hashable = frozenset(item_data.items())
        itemset_support_count[item_hashable] += 1

# Function to prune infrequent itemsets
def prune_infrequent_itemsets():
    global itemset_support_count
    itemset_support_count = {itemset: support for itemset, support in itemset_support_count.items() if support / total_transactions_count >= min_support_threshold}
    return itemset_support_count

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

# Consume messages from the Kafka topic
for message_data in consumer_instance:
    try:
        # Decode the message from bytes to string
        message = message_data.value.decode('utf-8')
        # Parsing the message string as JSON
        transaction_data = json.loads(message)
        
        # Update the support count of itemsets
        update_support_count(transaction_data)
        
        # Increment the total number of transactions processed
        total_transactions_count += 1
        
        # Prune infrequent itemsets
        items = prune_infrequent_itemsets()
        
        # Print 
        print("Frequent itemsets:", items)
        save_to_MongoDB(items)
    
    except Exception as e:
        print(f"Error processing message: {e}")

# Close the Kafka consumer
consumer_instance.close()
