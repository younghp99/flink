package zbank.flink

import java.util.Properties

import java.util.Properties
import com.alibaba.fastjson.JSON
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.api.scala._

object kafkaConsumser {
  def main(args: Array[String]) {
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

    case class rsltData(user:String,tranamt:Int)
    // 消费kafka数据
    val consumer = new FlinkKafkaConsumer[String](TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    consumer.setStartFromLatest()
    val stream=env.addSource(consumer)
    //次数
    val wordCount=stream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(_._1).sum(1)

    val tranAmt=stream.map(x=>(x.split(',')(0),x.split(',')(1).toInt,1))
    //客群分类
    val isGood=tranAmt.map(value=>{
      if(value._2>50){(value._1,value._2,"Good")}
      else{(value._1,value._2,"Bad")}
    })
    //isGood.print("333")
    //交易总金额
    val totalAmt=tranAmt.keyBy(_._1).sum(1)
    //totalAmt.print("444")
    //wordCount.print().setParallelism(2)
    //平均交易金额
    val avgAmt=tranAmt.map(x => {
      (x._1, x._2, 1)
    }).keyBy(_._1).sum(1)
    val totalCnt=tranAmt.map(x=>(x._1,x._2,x._3)).keyBy(1).sum(2)
    avgAmt.print("555")
    totalCnt.print("666")
    println("开始消费kafka：")
    env.execute("kafka_flink")
    println("结束消费kafka：")
  }
}
