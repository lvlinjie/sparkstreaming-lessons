//package lesson10
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object KafkaDirect {
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
//     * val directKafkaStream = KafkaUtils.createDirectStream[
//     * [key class], [value class], [key decoder class], [value decoder class] ](
//     *
//     * streamingContext, [map of Kafka parameters], [set of topics to consume])
//     *
//     */
//    val parameters=Map[String,String](
//      "bootstrap.servers" -> "192.168.167.254:9092",
//      "group.id" -> "testa"
//    )
//
//    val topics="flink".split(",").toSet
//    //这个就是比较接近企业里面的代码了。
//    //如果档次低一点的企业，实际上代码跟我们现在写的其实差不多。
//    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, parameters, topics)
//
//    //需要累加的效果
//    val result = kafkaStream.map(_._2).flatMap(_.split(","))
//      .map((_, 1))
//      .reduceByKey(_ + _)
//
//    //我需要把结果存储到HBASE
//    //foreachRDD
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//    ssc.stop()
//  }
//
//}
