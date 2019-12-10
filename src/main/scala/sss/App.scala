package sss

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object App {
  def main(args: Array[String]): Unit = {
    // Spark使用log4j打印日志，为了避免程序执行过程中产生过多的日志，添加如下两行代码
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 先创建SparkConf，再通过SparkConf创建SparkContext
    val conf = new SparkConf().setAppName("demo").setMaster("local")
    val sc = new SparkContext(conf)

//    // 进行词频统计
//    val rdd = sc.textFile("hdfs://qujianlei:9000/data/data.txt").
//      flatMap(_.split(" ")).
//      map(x => (x, 1)).
//      reduceByKey(_ + _).
//      saveAsTextFile("hdfs://qujianlei:9000/output/spark/0214")
    sc.stop()

  }
}
