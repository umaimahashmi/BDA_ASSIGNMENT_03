from kafka import KafkaConsumer
import json
from collections import defaultdict
from itertools import combinations

# Kafka broker settings
bootstrap_servers = 'localhost:9092'
topic = 'testtopic1'

# Create Kafka Consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# SON parameters
sample_size = 10  # Adjust based on your dataset and memory constraints
min_support_son = 1  # Minimum support threshold for SON algorithm

# Initialize dictionary for counting pair occurrences
pair_counts = defaultdict(int)

# Function to extract itemsets from messages
def extract_itemsets(message):
    itemset_str = message.get("also_buy", "")
    # Remove the quotes and brackets from the string representation
    itemset_str = itemset_str.replace("'", "").replace("[", "").replace("]", "")
    # Split the cleaned string into items and remove leading/trailing spaces
    itemset = [item.strip() for item in itemset_str.split(",") if itemset_str]
    return itemset

# Function to count pair occurrences in the sample
def count_pairs_in_sample(sample):
    for itemset in sample:
        pairs = list(combinations(itemset, 2))  # Generate pairs from the itemset
        for pair in pairs:
            pair_counts[pair] += 1

# Function to generate candidate pairs using SON algorithm
def generate_candidate_pairs(sample, k):
    candidate_pairs = defaultdict(int)
    for itemset in sample:
        pairs = list(combinations(itemset, 2))  # Generate pairs from the itemset
        for candidate in combinations(pairs, k):
            candidate_pairs[candidate] += 1
    return candidate_pairs

# Function to filter frequent pairs based on minimum support
def filter_frequent_pairs(candidate_pairs, min_support):
    frequent_pairs = {pair: support for pair, support in candidate_pairs.items() if support >= min_support}
    return frequent_pairs

# Function to apply SON algorithm on the messages
def apply_son(messages, sample_size, min_support):
    sample = []
    for message in messages:
        itemset = extract_itemsets(message)
        sample.append(itemset)
        if len(sample) == sample_size:
            count_pairs_in_sample(sample)
            sample_candidate_pairs = generate_candidate_pairs(sample, 2)
            frequent_pairs_pass1 = filter_frequent_pairs(sample_candidate_pairs, min_support)
            sample.clear()  # Clear the sample for the next iteration
    
    # Check for any remaining items in the last incomplete sample
    if sample:
        count_pairs_in_sample(sample)
    
    return frequent_pairs_pass1

# Main function to consume messages and apply SON algorithm
def consume_data():
    messages = []
    max_messages_to_process = 500 # Limit the number of messages to process
    
    for idx, message in enumerate(consumer):
        raw_data = message.value
        print("Raw data received:", raw_data)
        
        itemset = extract_itemsets(raw_data)
        print("Extracted itemset:", itemset)
        
        if itemset:
            # Remove any empty strings or None values from the itemset
            itemset = [item for item in itemset if item]
            messages.append({'items': itemset})
        
        if idx + 1 >= max_messages_to_process:
            print("Reached maximum messages to process. Exiting loop.")
            break  # Exit the loop if the maximum number of messages is reached
    
    print("Total messages collected:", len(messages))
    
    frequent_pairs = apply_son(messages, sample_size, min_support_son)
    
    # Print frequent pairs
    print("Frequent pairs found using SON algorithm:")
    for pair, support in frequent_pairs.items():
        print(f"{pair}: {support}")

if __name__ == '__main__':
    consume_data()

