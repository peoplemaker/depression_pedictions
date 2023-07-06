package com.soft.client

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import util.DBTools
object YearPerFemaleDepressedPedictions {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("LinearRegressionExample")
      .master("local")
      .getOrCreate()

    // 定义年份和抑郁症患者总数的数据
    val data = spark.read.option("delimiter", ",")
      .option("header", true)
      .option("multiLine", true)
      .csv("input/year_per_female_depressed.csv")

    val filteredData = data.filter(col("year").isNotNull
      && col("total_depressed").isNotNull
      && col("total_depressed")>10000)


    // 创建DataFrame
    import spark.implicits._
    val df = data.toDF("year", "total_depressed")
    val dfWithNumericYear = df.withColumn("year", $"year".cast("integer"))
    val dfWithNumericTotalDepressed = dfWithNumericYear.withColumn("total_depressed", $"total_depressed".cast(DoubleType))
    // 创建特征向量列
    val assembler = new VectorAssembler()
      .setInputCols(Array("year"))
      .setOutputCol("features")

    val assembledData = assembler.transform(dfWithNumericTotalDepressed)

    // 创建线性回归模型对象
    val lr = new LinearRegression()
      .setLabelCol("total_depressed")
      .setFeaturesCol("features")

    // 拟合模型
    val model = lr.fit(assembledData)

    // 打印回归系数和截距
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // 进行预测
    val newData = spark.range(1985, 2025, 1)
      .select(col("id").cast("int").as("year"))
    val predictions = model.transform(assembler.transform(newData))

    // 打印预测结果
    predictions.show(false)

    // 将数据扁平化
    val flattenedData = predictions.select($"year", $"prediction")
    flattenedData.show()
    DBTools.WriteMySql("year_per_female_depressed_predictions",flattenedData)
    // 关闭SparkSession
    spark.stop()
  }
}
