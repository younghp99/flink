1、zookeeper:
E:\modules\apache-zookeeper-3.7.0-bin
启动：
zkServer

2、kafka
E:\modules\kafka_2.11-2.3.0
启动：
.\bin\windows\kafka-server-start.bat .\config\server.properties



新建topic：
cd E:\modules\kafka_2.11-2.3.0\bin\windows
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic haha

创建生产者：
cd E:\modules\kafka_2.11-2.3.0\bin\windows
kafka-console-producer.bat --broker-list localhost:9092 --topic haha

创建消费者：
cd E:\modules\kafka_2.11-2.3.0\bin\windows
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic haha --from-beginning

查看topic：
cd E:\modules\kafka_2.11-2.3.0\bin\windows
kafka-topics.bath --list --zookeeper localhost:2181


kafka-consumer-groups.bat --bootstrap-server localhost:9092 --list --haha-consumer