//package lesson13
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object DirectKafkaWordCount {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    //步骤一：初始化程序入口
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ReceiverKafkaWordCount")
//    val ssc = new StreamingContext(sparkConf, Seconds(5))
//
//    val kafkaParams =  Map[String, String](
//      "bootstrap.servers"->"hadoop2:9092",
//      "group.id" -> "testflink"
//    )
//    val topics = "flink".split(",").toSet
//    //  ssc: StreamingContext,
//    //      kafkaParams: Map[String, String],
//    //      topics: Set[String]String
//    //k,v
//    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//      .map(_._2)
//    val result = lines.flatMap(_.split(",")).map((_, 1))
//      .reduceByKey(_ + _)
//
//    result.print()
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//
//
//  }
//
//}
