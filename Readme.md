# Kafka-offset-management using mysql

Storing offsets in zookeeper was the default behaviour in Kafka < 0.8.1 for the high level consumer. Zookeeper may become a bottleneck under heavy load. Storing offsets in zookeeper is highly non-advisable. However, this only occurs in extreme cases, when there are many hundreds of consumers using the same ZooKeeper cluster for offset management. 