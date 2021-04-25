package zbank.flink
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows

object JoinFlinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStateBackend(new FsStateBackend("file:///E:/project_code/data/tmp"))

    //kafka配置
    val ZOOKEEPER_HOST = "localhost:2181"
    val KAFKA_BROKERS = "localhost:9092"
    val TRANSACTION_GROUP = "flink-helper-label-count"
    //val TOPIC_NAME = "tongji-flash-hm2-helper"
    val L_TOPIC_NAME = "left"
    val R_TOPIC_NAME = "right"
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    // watrmark 允许数据延迟时间
    val MaxOutOfOrderness = 86400 * 1000L
    // 消费kafka数据
    val lconsumer = new FlinkKafkaConsumer[String](L_TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    lconsumer.setStartFromLatest()
    val lstream=env.addSource(lconsumer)
    // 消费kafka数据
    val rconsumer = new FlinkKafkaConsumer[String](R_TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    rconsumer.setStartFromLatest()
    val rstream=env.addSource(rconsumer)
    //处理left流数据
    print("开始处理kafka数据：")
    val ldata=lstream.map(x=>{(x,1)})
    val rdata=rstream.map(x=>{(x,1)})
    ldata.print()
    rdata.print()
    env.execute("kafkajoin")
  }
}
