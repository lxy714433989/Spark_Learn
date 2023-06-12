package com.lxy.spark.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WordCountDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCountDemo")
    val sparkContext = new SparkContext(sparkConf)
    //获取文件
    val file: RDD[String] = sparkContext.textFile(args(0))
    val split: RDD[String] = file.flatMap(_.split(" "))
    val map: RDD[(String, Int)] = split.map(word => (word, 1))
    val reduce: RDD[(String, Int)] = map.reduceByKey(_ + _)
//    reduce.saveAsTextFile("output")
    val res: Array[(String, Int)] = reduce.collect()
    res.foreach(println)
    sparkContext.stop()
  }
}
