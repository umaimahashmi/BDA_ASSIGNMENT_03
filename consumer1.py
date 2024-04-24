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

# Function to extract itemsets from messages
def extract_itemsets(message):
    # Extract itemset from message
    also_buy_str = message.get("also_buy", "")
    if also_buy_str:
        # Extract items from the string representation
        items = also_buy_str.strip("[]").replace("'", "").split(", ")
        itemset = set(items)
    else:
        itemset = set()
    return itemset

# Function to generate candidate k-itemsets from frequent (k-1)-itemsets
def generate_candidates(prev_frequent_itemsets, k):
    candidates = set()
    for itemset1 in prev_frequent_itemsets:
        for itemset2 in prev_frequent_itemsets:
            if len(itemset1.union(itemset2)) == k and itemset1 != itemset2:
                new_candidate = itemset1.union(itemset2)
                candidates.add(tuple(sorted(new_candidate)))  # Convert to tuple for hashability
    return candidates

# Function to generate frequent itemsets using Apriori algorithm
def apriori(transactions, min_support):
    frequent_itemsets = defaultdict(int)
    k = 1
    while True:
        candidate_itemsets = defaultdict(int)
        for transaction in transactions:
            for itemset in combinations(transaction, k):
                candidate_itemsets[itemset] += 1

        frequent_itemsets_this_round = {itemset: support for itemset, support in candidate_itemsets.items() if support >= min_support}

        if not frequent_itemsets_this_round:
            break

        frequent_itemsets.update(frequent_itemsets_this_round)
        print(f"Frequent {k}-itemsets:",frequent_itemsets_this_round)
    
        k += 1

    return frequent_itemsets

# Main function to consume messages and apply Apriori algorithm with sliding window
def consume_data(window_size=5, min_support=2):
    transactions_window = []
    transactions_all = []

    for message in consumer:
        data = message.value
        itemset = extract_itemsets(data)
        transactions_window.append(itemset)
        transactions_all.append(itemset)

        if len(transactions_window) == window_size:
            # Apply Apriori algorithm on the current window
            apriori(transactions_window, min_support)

            # Slide the window
            transactions_window.pop(0)

    # Apply Apriori algorithm on the remaining transactions if any
    if transactions_window:
        apriori(transactions_window, min_support)

    # Apply Apriori algorithm on all transactions at the end
    print("Applying Apriori on all transactions:")
    apriori(transactions_all, min_support)

if __name__ == '__main__':
    consume_data()
