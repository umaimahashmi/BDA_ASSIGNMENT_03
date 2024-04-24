from collections import defaultdict
from itertools import combinations
from kafka import KafkaConsumer
import json

# Kafka broker settings
bootstrap_servers = 'localhost:9092'
topic = 'testtopic1'

# Create Kafka Consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# PCY parameters
hash_table_size = 1000  # Adjust based on your dataset and memory constraints
support_threshold = 1  # Adjust based on your dataset and requirements
buffer_size = 1000  # Adjust based on your dataset and memory constraints

# Initialize hash table for counting pairs and bitmap for hash collisions
hash_table = defaultdict(int)
bitmap = [0] * hash_table_size

# Sliding window parameters
window_size = 100
sliding_window = []

# Function to extract itemsets from messages
def extract_itemsets(message):
    itemset_str = message.get("also_buy", "")
    # Remove the quotes and brackets from the string representation
    itemset_str = itemset_str.replace("'", "").replace("[", "").replace("]", "")
    # Split the cleaned string into items and remove leading/trailing spaces
    itemset = [item.strip() for item in itemset_str.split(",") if itemset_str]
    return itemset

# Function to generate pairs from the given itemset
def generate_pairs(itemset):
    pairs = []
    for i in range(len(itemset)):
        for j in range(i + 1, len(itemset)):
            pairs.append((itemset[i], itemset[j]))
    return pairs

# Function to apply PCY algorithm on the pairs
def apply_pcy(pairs):
    for pair in pairs:
        hash_value = hash(pair) % hash_table_size
        if bitmap[hash_value] == 1:
            hash_table[hash_value] += 1
        bitmap[hash_value] += 1

# Function to filter frequent pairs based on support threshold
def filter_frequent_pairs():
    frequent_pairs = []
    for pair, count in hash_table.items():
        if count >= support_threshold:
            frequent_pairs.append((pair, count))
    return frequent_pairs

# Function to update sliding window and process data
def update_sliding_window(transaction):
    if len(sliding_window) >= window_size:
        oldest_transaction = sliding_window.pop(0)
        # Remove oldest transaction from hash table and bitmap
        pairs_to_remove = generate_pairs(oldest_transaction)
        for pair in pairs_to_remove:
            hash_value = hash(pair) % hash_table_size
            if bitmap[hash_value] > 0:
                bitmap[hash_value] -= 1
                if bitmap[hash_value] == 0:
                    hash_table[hash_value] = 0
    
    # Add new transaction to sliding window
    sliding_window.append(transaction)

# Main function to consume messages and process data
def consume_data():
    for message in consumer:
        raw_data = message.value
        print("Raw data:", raw_data)
        
        itemset = extract_itemsets(raw_data)
        print("Extracted itemset:", itemset)
        
        if itemset:
            # Remove any empty strings or None values from the itemset
            itemset = [item for item in itemset if item]
            pairs = generate_pairs(itemset)
            print("Generated pairs:", pairs)
            
            update_sliding_window(itemset)  # Update sliding window
            apply_pcy(pairs)
            frequent_pairs = filter_frequent_pairs()
            print("Frequent pairs:", frequent_pairs)
        else:
            print("No 'also_buy' items found.")

if __name__ == '__main__':
    consume_data()
