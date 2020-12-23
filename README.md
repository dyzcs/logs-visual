# logs-visual
 2020 大数据分析及可视化课设

## kafka 语句
1. **新建主题**
> kafka-topics.sh -create --zookeeper s183:2181 --partitions 1 --replication-factor 1 --topic logs_startup

> kafka-topics.sh -create --zookeeper s183:2181 --partitions 1 --replication-factor 1 --topic logs_event

2. **启动**
> kafka-console-consumer.sh --bootstrap-server s183:9092 --topic logs_startup

> kafka-console-consumer.sh --bootstrap-server s183:9092 --topic logs_event