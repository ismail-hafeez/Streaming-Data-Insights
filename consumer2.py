from pymongo import MongoClient
from kafka import KafkaConsumer
import json
import itertools
import numpy as np

bootstrap_servers = ['localhost:9092']
topic = 'test-topic'

consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

class PCY:
    def __init__(self, transactions: list, support: int, chunk: int, size_of_bucket: int):
        self.transactions = transactions
        self.support = (support / 100) * len(transactions) * (chunk / 100)
        self.size_of_bucket = size_of_bucket
        self.last_line = int(len(transactions) * (chunk / 100))
        self.frequent_itemsets = set()

    @staticmethod
    def hash_pair(pair: tuple) -> int:
        return hash(pair) % 50021

    def update_frequency(self, count_frequency: dict, basket: list) -> dict:
        for item in basket:
            if item not in count_frequency:
                count_frequency[item] = 1
            else:
                count_frequency[item] += 1
        return count_frequency

    def update_bucket(self, basket: list, bucket: np.ndarray) -> np.ndarray:
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            bucket_num = self.hash_pair(pair) % self.size_of_bucket
            if bucket[bucket_num] < self.support:
                bucket[bucket_num] += 1
        return bucket

    def get_frequent_items(self, count_frequency: dict) -> list:
        frequent_items = [item for item, count in count_frequency.items() if count >= self.support]
        self.frequent_itemsets.update(frequent_items)
        return frequent_items

    def create_candidate_pairs(self, frequent_items: list, bucket: np.ndarray) -> dict:
        all_pairs = itertools.combinations(frequent_items, 2)
        candidate_pairs = dict()
        for pair in all_pairs:
            if bucket[self.hash_pair(pair) % self.size_of_bucket] >= self.support:
                candidate_pairs[pair] = 0
        return candidate_pairs

    def update_candidate_pairs(self, candidate_pairs: dict, basket: list) -> dict:
        pairs = itertools.combinations(basket, 2)
        for pair in pairs:
            if pair in candidate_pairs:
                candidate_pairs[pair] += 1
        return candidate_pairs

    def basic_pcy(self):
        bucket = np.zeros(self.size_of_bucket, dtype=int)
        count_frequency = dict()
        for line_num in range(self.last_line):
            basket = self.transactions[line_num]
            count_frequency = self.update_frequency(count_frequency, basket)
            bucket = self.update_bucket(basket, bucket)

        frequent_items = self.get_frequent_items(count_frequency)
        candidate_pairs = self.create_candidate_pairs(frequent_items, bucket)

        for line_num in range(self.last_line):
            basket = self.transactions[line_num]
            candidate_pairs = self.update_candidate_pairs(candidate_pairs, basket)
        return frequent_items

class KafkaPCYIntegration:
    def __init__(self, support: int, chunk: int, sizeOfBucket: int):
        self.size_of_bucket = sizeOfBucket
        self.support = support
        self.chunk = chunk

        # MongoDB connection details
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['mydatabase']
        self.collection = self.db['frequent_itemsets']

    def process_data(self, data):
        frequent_itemsets = set()
        for message in data:
            try:
                transactions = message['also_buy']
                pcy = PCY(transactions, self.support, self.chunk, self.size_of_bucket)
                frequent_itemsets.update(pcy.basic_pcy())
            except TypeError as e:
                print(f"Error processing message: {e}. Skipping...")
                continue
        
        # Insert frequent itemsets into MongoDB collection
        for itemset in frequent_itemsets:
            self.collection.insert_one({'itemset': itemset})

# Define the support, chunk, and sizeOfBucket parameters
support = 10  # Adjust the support threshold as needed
chunk = 1000  # Adjust the chunk size as needed
sizeOfBucket = 50000  # Adjust the size of the bucket as needed

# Create an instance of KafkaPCYIntegration
integration = KafkaPCYIntegration(support, chunk, sizeOfBucket)

# Continuously consume messages from Kafka and process them
for message in consumer:
    data = message.value
    integration.process_data(data)

