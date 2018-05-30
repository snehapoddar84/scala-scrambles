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
import com.spoddar.caseclasses.DATA

object FindDelta {

  /**
   * Return the most Recent
   */
  def findDelta(dataset1: DataFrame, dataset2: DataFrame): DataFrame = {
    val id1 = dataset1.select($"user_id")
    val id2 = dataset2.select($"user_id")

    val insertID = id2.except(id1)
    val insertIDList = insertID.select("user_id").map(r => r.getLong(0)).collect.toList
    val insertRecords = dataset2.filter($"user_id".isin(insertIDList: _*))
    val insertRecordswithStatus = insertRecords.withColumn("delta_type", lit("inserted"))

    val deletedID = id1.except(id2)
    val deletedIDList = deletedID.select("user_id").map(r => r.getLong(0)).collect.toList
    val deletedRecords = dataset1.filter($"user_id".isin(deletedIDList: _*))
    val deletedRecordswithStatus = deletedRecords.withColumn("delta_type", lit("deleted"))

    val updatedID = id1.intersect(id2)
    val updatedIDList = updatedID.select("user_id").map(r => r.getLong(0)).collect.toList
    val records = dataset2.filter($"user_id".isin(updatedIDList: _*))
    val updatedRecords = records.except(dataset1)
    val updatedRecordswithStatus = updatedRecords.withColumn("delta_type", lit("updated"))

    // Join all records
    val recordsWithStatus = insertRecordswithStatus.unionAll(deletedRecordswithStatus).unionAll(updatedRecordswithStatus)
    recordsWithStatus
  }

  def main(args: Array[String]): Unit = {
    println("FindDelta Started")

    val sparkConf = new SparkConf().setAppName("FindRecent")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits

    val dataList1 = List(
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

    val dataset1 = sqlContext.createDataFrame(dataList1)

    dataset1.show(false)

    val dataList2 = List(
      DATA(2l, Date.valueOf("2018-05-14"), 10),
      DATA(3l, Date.valueOf("2018-05-17"), 10),
      DATA(5l, Date.valueOf("2018-05-17"), 10),
      DATA(1l, Date.valueOf("2018-05-15"), 20),
      DATA(2l, Date.valueOf("2018-05-15"), 20),
      DATA(6l, Date.valueOf("2018-05-15"), 20),
      DATA(3l, Date.valueOf("2018-05-16"), 20),
      DATA(7l, Date.valueOf("2018-05-16"), 20),
      DATA(3l, Date.valueOf("2018-05-17"), 200),
      DATA(4l, Date.valueOf("2018-05-17"), 400),
      DATA(1l, Date.valueOf("2018-05-16"), 100),
      DATA(5l, Date.valueOf("2018-05-17"), 650),
      DATA(5l, Date.valueOf("2018-05-16"), 200),
      DATA(1l, Date.valueOf("2018-05-17"), 400),
      DATA(2l, Date.valueOf("2018-05-17"), 200),
      DATA(7l, Date.valueOf("2018-05-17"), 5),
      DATA(8l, Date.valueOf("2018-05-17"), 5),
      DATA(9l, Date.valueOf("2018-05-17"), 5),
      DATA(5l, Date.valueOf("2018-05-17"), 5))

    val dataset2 = sqlContext.createDataFrame(dataList2)

    dataset2.show(false)

    findDelta(dataset1, dataset2).show(8)

    // Stop spark context
    sc.stop
    println("FindDelta Completed")
  }

}