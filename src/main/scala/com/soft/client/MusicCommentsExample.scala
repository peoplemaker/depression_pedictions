package com.soft.client


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import util.DBTools


object MusicCommentsExample {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSqlCSVExample").setMaster("local")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    var df = spark.read.format("csv").option("header",true).option("multiLine",true).load("F:\\scsx\\sparksql\\input\\comments.csv")
    df.createOrReplaceTempView("tbl")

    val result = spark.sql("select * from tbl")
    result.show()
    DBTools.WriteMySql("music_comments",result)
    }
  }
