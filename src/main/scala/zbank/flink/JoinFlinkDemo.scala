package zbank.flink
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

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
    val TOPIC_NAME = "haha"
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    // watrmark 允许数据延迟时间
    val MaxOutOfOrderness = 86400 * 1000L


  }
}
