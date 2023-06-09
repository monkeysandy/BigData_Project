# PA3 Kafka & Spark

## Part 1
**Determine 100 most frequent/repeated words in the 16GB dataset**

### How to run it
1. run at local
```python
python3 wc_spark.py
```

2. run at spark
```python
spark-submit --master local wc_spark.py
```

## Part 2
### Section 1
**Solving and getting familiar with log analytics.**

1. run section1.py file to get the freq day of the week and least year for the data

### Section 2
#### Basic
1. run Kafka and ZooKeeper
*make sure to get into kafka file location*

```bash
# Start the ZooKeeper service
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties

```
2. create a topic to store your event

```bash
bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
```
3. show you details such as the partition count of the new topic

```bash
bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
```
4. write some events into topic

```bash
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
```
5. read the events

```bash
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```
#### How to run
1. Run Kafka & Zookeeper
2. Create topic
3. Run kafka_producer.py to input the data into topic
4. Run kafka_consumer.py to save the analysis result into hdfs
