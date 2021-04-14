import java.util.Properties
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import com.alibaba.fastjson.JSON
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
    println("开始消费kafka：")
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
    // 消费kafka数据
    val consumer = new FlinkKafkaConsumer[String](TOPIC_NAME, new SimpleStringSchema(), kafkaProps)
    //consumer.setStartFromLatest()
    val stream=env.addSource(consumer)
    //计算次数
    val wordCount=stream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(_._1).sum(1)
    //根据交易金额分群
    val tranAmt=stream.map(x=>(x.split(',')(0),x.split(',')(1).toInt)).map(value=>{
      if(value._2>50){(value._1,value._2,"Good")}
      else{(value._1,value._2,"Bad")}
    })
    tranAmt.print()
    //wordCount.print().setParallelism(2)
    //计算平均交易金额
    val avgAmt=stream.map(x=>(x.split(",")(0),x.split(",")(1).toInt)).keyBy(_._1)
    env.execute("kafka_flink")


  }

}
