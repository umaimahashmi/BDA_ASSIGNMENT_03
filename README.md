# BDA_ASSIGNMENT_02

A repository created for a big data assignment.

## GROUP MEMBERS

1. **Umaima Hashmi** &nbsp;&nbsp;&nbsp;22i-1894   
2. **Nooran Ishtiaq** &nbsp;&nbsp;&nbsp;22i-2010   
3. **Manhab Zafar** &nbsp;&nbsp;&nbsp;22i-1957  

---

## Pre-processing

We used **DASK LIBRARY** for pre-processing, allowing batch processing and efficient handling of large datasets. Previously, using pandas in our assignment took 20 - 30 mins for pre-processing. DASK is specifically designed to speed up work on large datasets, such as our 15 GB dataset.

---

## Consumer 1: Apriori Algorithm Implementation

This demonstration implements the Apriori algorithm, a classic in data mining for finding frequent itemsets in transactional data.

- **Objective:** Identify frequent itemsets within a set of transactions.
- **Approach:** Utilizes a sliding window technique for batch processing and Apriori algorithm application.
- **Algorithm Steps:**
  - Extract Itemsets
  - Sliding Window
  - Apriori Implementation
- **Output:** Prints frequent k-itemsets and overall frequent itemsets across all transactions.

**Key Functions:**
- extract_itemsets: Extracts itemsets from JSON messages.
- generate_candidates: Generates candidate k-itemsets.
- apriori: Implements the Apriori algorithm.
- consume_data: Main function for consuming messages and applying Apriori.

---

## Consumer 2: PCY Algorithm Implementation

This script demonstrates the PCY (Pair Counting with Bitmaps) algorithm for identifying frequent pairs from transactional data in Kafka.

- **Objective:** Discover frequent item pairs efficiently using a hash table and bitmap.
- **Components:**
  - Hash Table
  - Bitmap
- **Algorithm Steps:**
  - Extract Itemsets
  - Generate Pairs
  - Apply PCY
  - Filter Frequent Pairs
  - Sliding Window

**Key Functions:**
- extract_itemsets: Extracts itemsets from JSON messages.
- generate_pairs: Generates pairs for PCY processing.
- apply_pcy: Applies the PCY algorithm.
- filter_frequent_pairs: Filters frequent pairs.
- update_sliding_window: Updates the sliding window efficiently.

---

## Consumer 3: SON Algorithm Implementation

This script demonstrates the SON (Savasere, Omiecinski, and Navathe) algorithm for finding frequent item pairs efficiently from Kafka data.

- **Objective:** Identify frequent pairs using a two-phase approach.
- **Components:**
  - Sample Size
  - Minimum Support
- **Algorithm Steps:**
  - Extract Itemsets
  - Apply SON on Sample
  - Generate Candidate Pairs
  - Filter Frequent Pairs
  - Apply SON on Entire Dataset

**Key Functions:**
- extract_itemsets: Extracts itemsets from JSON messages.
- count_pairs_in_sample: Counts pair occurrences in the sample.
- generate_candidate_pairs: Generates candidate pairs.
- filter_frequent_pairs: Filters frequent pairs.
- apply_son: Applies the SON algorithm.

