package com.lxy.spark.demo

import org.apache.spark.sql.SparkSession

object SQLToRDDExample {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    System.setProperty("spark.master", "local")
    val spark = SparkSession.builder()
      .appName("SQL to RDD")
      .getOrCreate()

    // 加载数据为DataFrame
    val dataframe = spark.read
      .option("header", "true")
      .csv("data.csv")

    // 注册表
    dataframe.createOrReplaceTempView("my_table")

    // 编写SQL查询
    val sqlQuery = "SELECT * FROM my_table"

    // 执行查询
    val resultDF = spark.sql(sqlQuery)
    resultDF.show()

    // 将DataFrame转换为RDD
    val resultRDD = resultDF.rdd

    val transformedRDD = resultRDD
      .map(row => row.getAs[String]("name")) // 提取指定列的值
      .filter(name => name.startsWith("zz")) // 过滤满足条件的数据
      .flatMap(name => name.split(" ")) // 将每个字符串拆分成单词
      .map(word => (word, 1)) // 将每个单词映射为键值对
      .reduceByKey(_ + _) // 按键合并计数

    transformedRDD.foreach(println)
  }
}
