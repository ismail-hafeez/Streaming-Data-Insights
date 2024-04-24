# Streaming-Data-Insights

# Streaming Data Insights

This project demonstrates real-time analysis of streaming data using Kafka and Apriori algorithm to identify frequent itemsets.

## Introduction

Streaming data analysis is crucial for gaining real-time insights from continuously generated data. This project leverages Apache Kafka for stream processing and implements various algorithms like Apriori, PCY, and ELCat to extract frequent itemsets from the data stream.

## Apriori Algorithm

The Apriori algorithm is a classic algorithm used for association rule mining in large datasets. It works by iteratively generating candidate itemsets and pruning those that do not meet the minimum support threshold.

## PCY (Park-Chan-Yu) Algorithm

The PCY algorithm is an optimization of the Apriori algorithm that uses a hash table to count the frequency of item pairs. It leverages the "hash-and-count" strategy to reduce memory consumption and improve efficiency.

## ELCat (ELimination and Combination A-priori Tree) Algorithm

The ELCat algorithm is another optimization of the Apriori algorithm that uses a tree-based structure to efficiently generate candidate itemsets. It eliminates the need to generate and test all possible combinations of itemsets, leading to faster execution.

## Setup

1. **Requirements:**
   - Python (>=3.6)
   - Apache Kafka
   - MongoDB

2. **Installation:**
   - Install Kafka and start the Kafka server.
   - Install Python dependencies using `

### Producer
- **File:** producer.py
- **Description:** Reads data from a JSON file and publishes it to a Kafka topic.

### Consumer
- **File:** consumer.py
- **Description:** Consumes data from the Kafka topic, applies the Apriori algorithm, and saves frequent itemsets to a local file.

## Usage

1. Start the Kafka server.
2. Run the producer.py script to publish data to the Kafka topic.
3. Run the consumer.py script to consume data, analyze it using Apriori, and save frequent itemsets.
4. View real-time insights in the console output and saved results in the output file.

## Configuration

- **Kafka Configuration:** Modify the Kafka server address and topic name in the producer and consumer scripts as per your setup.
- **Apriori Parameters:** Adjust the minimum support threshold in the consumer script to control the sensitivity of the algorithm.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

