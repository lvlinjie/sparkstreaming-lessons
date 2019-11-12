import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //conf.setMaster("local[2]")
    conf.setAppName("test")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val rdd: RDD[String] = spark.sparkContext.textFile("hdfs://10.148.15.6:8020/tmp/hello.txt")

    rdd.foreach( r =>{
      println(r)
    })
    spark.close()

  }

}
