//package lesson09
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object KafkaByReceiver {
//  def main(args: Array[String]): Unit = {
//    //设置了日志的级别
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    //1. 数据的输入
//    //步骤一：创建程序入口
//    val conf = new SparkConf()
//
//    //driver executor task
//
//    //如果写的是local那么代码的就是1个线程
//    //但是这儿至少需要2个线程才能跑起来，因为一个线程要接收数据，一个线程要处理数据。
//    //local[*] 你当前的电脑有多少个cpu core * 就代表是几
//    conf.setMaster("local[*]")
//    conf.setAppName("word count")
//    val ssc = new  StreamingContext(conf,Seconds(3))
//
//    /**
//     * ssc: StreamingContext,
//     * kafkaParams: Map[String, String],
//     * topics: Map[String, Int],
//     * storageLevel: StorageLevel
//     */
//    val kafkaParams=Map[String,String](
//      "zookeeper.connect" -> "192.168.167.254:2181,192.168.167.248:2181,192.168.167.249:2181/kafka",
//      "group.id" -> "test"
//    )
//    val topics = "flink".split(",").map((_,1)).toMap
//    //k,v  -> k,v
//    val KafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,
//      kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER)
//
//    val result = KafkaStream.map(_._2).flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//  }
//
//}
