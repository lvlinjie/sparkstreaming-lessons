//package lesson12
//
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object ReceiverKafkaWordCount {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    //步骤一：初始化程序入口
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ReceiverKafkaWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val kafkaParams =  Map[String, String](
//      "zookeeper.connect"->"hadoop2:2181,hadoop3:2181,hadoop1:2181",
//      "group.id" -> "testflink"
//    )
//
//    val topics = "xiaoxian".split(",").map((_,1)).toMap
//    //步骤二：获取数据源
//    //默认只会有一个receiver
//
//    val lines = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
//     ssc,kafkaParams,topics, StorageLevel.MEMORY_AND_DISK_SER)
//
////    val kafkaStreams = (1 to 3).map(_ => {
////  KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
////    ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER)
////})
////    val lines = ssc.union(kafkaStreams)
//
//    //步骤三：业务代码处理
//    lines.map(_._2).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
