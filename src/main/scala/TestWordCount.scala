import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("test")
    //conf.setMaster("local[2]")

    //Application -> Job
    val ssc = new StreamingContext(conf,Seconds(2))

    val dataStream = ssc.socketTextStream("localhost",8888)
    val result = dataStream.flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
