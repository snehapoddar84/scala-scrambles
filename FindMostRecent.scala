package com.spoddar.utils

import java.sql.Date
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkConf

case class DATA(
  user_id: Long,
  date: java.sql.Date,
  score: Integer)

object FindMostRecent {

  /**
   * Return the most Recent
   */
  def markMostRecent(dataset: DataFrame): DataFrame = {

    // Add the rowNumber columns for each user_id group
    val datasetWithRN = dataset.withColumn("row_num", row_number().over(Window.partitionBy("user_id")))

    // Create a window partition function
    val windowSpec = Window.partitionBy("user_id").orderBy(desc("row_num"))

    // Add the rank columns, per the window partition function
    val rankedData = datasetWithRN.withColumn("dense_rank", dense_rank().over(windowSpec))

    // Add the status column to mark most recent entry
    val dataWithStatusColumn = rankedData.withColumn("status", when(col("dense_rank") === 1, "most_recent").otherwise(""))

    dataWithStatusColumn

  }

  def main(args: Array[String]): Unit = {
    println("FindRecent Started")

    val sparkConf = new SparkConf().setAppName("FindRecent")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits

    val dataList = List(
      DATA(1l, Date.valueOf("2018-05-14"), 10),
      DATA(2l, Date.valueOf("2018-05-14"), 10),
      DATA(3l, Date.valueOf("2018-05-17"), 10),
      DATA(4l, Date.valueOf("2018-05-17"), 10),
      DATA(5l, Date.valueOf("2018-05-17"), 10),
      DATA(1l, Date.valueOf("2018-05-15"), 20),
      DATA(2l, Date.valueOf("2018-05-15"), 20),
      DATA(3l, Date.valueOf("2018-05-16"), 20),
      DATA(4l, Date.valueOf("2018-05-17"), 20),
      DATA(5l, Date.valueOf("2018-05-16"), 20),
      DATA(3l, Date.valueOf("2018-05-17"), 100),
      DATA(4l, Date.valueOf("2018-05-17"), 100),
      DATA(1l, Date.valueOf("2018-05-16"), 100),
      DATA(2l, Date.valueOf("2018-05-17"), 100),
      DATA(5l, Date.valueOf("2018-05-17"), 150),
      DATA(5l, Date.valueOf("2018-05-16"), 200),
      DATA(3l, Date.valueOf("2018-05-16"), 300),
      DATA(1l, Date.valueOf("2018-05-17"), 300),
      DATA(2l, Date.valueOf("2018-05-17"), 200),
      DATA(5l, Date.valueOf("2018-05-17"), 5))

    val dataset = sqlContext.createDataFrame(dataList)

    dataset.show(false)

    markMostRecent(dataset).show(8)

    // Stop spark context
    sc.stop
    println("FindRecent Completed")
  }

}
