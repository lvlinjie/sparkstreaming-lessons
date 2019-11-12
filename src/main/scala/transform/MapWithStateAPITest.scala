package transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

object MapWithStateAPITest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("C:\\Users\\Administrator\\Desktop\\SparkStreaming精讲\\checkpointdata")

    val lines = ssc.socketTextStream("hadoop1", 8888)

    val words = lines.flatMap(_.split(","))
    val wordsDStream = words.map(x => (x, 1))

    val initialRDD = sc.parallelize(List(("flink", 100L), ("storm", 32L)))
    // currentBatchTime : 表示当前的Batch的时间
    // key: 表示需要更新状态的key
    // value: 表示当前batch的对应的key的对应的值
    // currentState: 对应key的当前的状态

    /**
      *
      * hadoop,1
      * hadoop,1
      * hadoop,1
      *
      * {hadoop,(1,1,1)  => 3}
      *
      * key:hadoop  当前的key
      * value:3  当前的key出现的次数
      *currentState： 当前的这个key的历史的状态
      *
      * hadoop:7
      *
      * hadoop,10
      *
      */

    val stateSpec =StateSpec.function((currentBatchTime: Time, key: String, value: Option[Int], currentState: State[Long]) => {

      val sum = value.getOrElse(0).toLong + currentState.getOption.getOrElse(0L)

      val output = (key, sum)
      //如果你的数据没有超时
      if (!currentState.isTimingOut()) {
        currentState.update(sum)
      }
      //最后一行代码是返回值
      Some(output)
    }).initialState(initialRDD).numPartitions(2).timeout(Seconds(15))
    //timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉

    /**
      * reduceByKey
      * udpateStateByKey
      * mapWithState
      */
    val result = wordsDStream.mapWithState(stateSpec)

   // result.print()

    result.stateSnapshots().print()

    //启动Streaming处理流
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
