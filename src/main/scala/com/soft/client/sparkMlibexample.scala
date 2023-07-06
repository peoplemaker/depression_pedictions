package com.soft.client

import org.apache.spark.ml.regression.LinearRegression
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.functions.expr
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType}

import scala.collection.immutable

/**
 * 机器学习
 */
object MachineWeatherExample1 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("MachineWeatherExample").setMaster("local")
    //2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    var data = spark.read.option("delimiter", ",")
      .option("header", true)
      .option("multiLine", true)
      .csv("input/countryTemperatures.csv")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //1.数据处理 正则表达式的替换
    data = regexpReplace(data, "dt", "/", "-")
    data.show(5)

    //2.数据清洗 删除不是空值的记录
    val filteredData = data.filter(col("dt").isNotNull
      && col("AverageTemperature").isNotNull
      && col("country").isNotNull
      && !col("country").like("%,%")
      && !col("country").like("%?%"))

    //3.数据加工
    //3.1 数据格式转换
    filteredData.withColumn("dt", (to_date(col("dt"), "yyyy-MM")))
    //3.2 日期按年、月拆分进行转换
    val featureData = filteredData.withColumn("year", year($"dt"))
      .withColumn("month", month($"dt"))
      .select("year", "month", "country", "AverageTemperature")
    featureData.show(10)

    //3.2 自然语言使用StringIndexer进行标签编码
    val indexer = new StringIndexer()
      .setInputCol("country")
      .setOutputCol("countryIndex")

    //3.3 拟合StringIndexer模型，生成编码器
    val codemodel = indexer.fit(featureData)
    val datamodel = codemodel.transform(featureData)
    datamodel.show(20)

    val labels = codemodel.labels
    val indices = codemodel.transform(featureData).select("countryIndex").distinct().collect().map(_.getDouble(0))
    val labelIndexMap = indices.zip(labels).toMap
    labelIndexMap.foreach {
      case (label, index) => println(s"$label: $index")
    }

    //3.4 生成新的数据集
    val result = datamodel.map(
      row => (row.getInt(0), row.getInt(1), row.getDouble(4), row.getString(3).toDouble)
    ).toDF("year", "month", "countryIndex", "temperature")

    //result.show(10)
    //3.5 数据预处理
    // 创建了一个 VectorAssembler 对象，并设置了输入列和输出列。setInputCols 方法接收一个字符串数组，
    // 参数是要组合成向量的列名，这里是 "year"、"month" 和 "countryIndex"。
    // setOutputCol 设置了输出列的名称为 "features"，该列将包含组合后的向量特征。
    val assembler = new VectorAssembler()
      .setInputCols(Array("year", "month", "countryIndex")) // 填写实际的日期特征列名
      .setOutputCol("features")
    //使用 transform 方法将 result中的列数组转换为向量列。transform方法接收一个输入DataFrame，
    //并根据之前设置的输入和输出列进行转换。转换后的结果保存在新的 DataFrame中，这里命名为assembledData。
    //然后，使用select方法选择了转换后的features列和原始的temperature列，生成最终的 DataFrame。
    val assembledData = assembler.transform(result).select("features", "temperature")

    //4.1拆分数据集为训练集和测试集（按比例拆分）
    val Array(trainingData, testData) = assembledData.randomSplit(Array(0.7, 0.3))
    trainingData.show(10)

    //4.2 创建线性回归模型
    val lr = new LinearRegression()
      .setLabelCol("temperature")
      .setFeaturesCol("features")

    //模型训练
    val model = lr.fit(trainingData)
    // 测试集进行预测
    val predictions: DataFrame = model.transform(testData)
    // 打印预测结果
    val predictionResult = predictions.select("prediction", "temperature", "features")
    predictionResult.show(10)


    var collect: Array[String] = predictionResult.map(_.toString).collect
    val arr1 = collect
    arr1.foreach(println)

    println("------------")
    val cleanedArray = arr1.map { stringData =>
      stringData.replaceAll("\\[|\\]", "")
    }
    cleanedArray.foreach(println)

    val dataArray = cleanedArray.map { stringData =>
      val data = stringData
        .split(",")
        .map(_.trim)

      Row(
        data(0).toDouble,
        data(1).toDouble,
        data(2).toDouble,
        data(3).toDouble,
        labelIndexMap.getOrElse(data(4).toDouble, "1.0")
      )
    }

    val schema = StructType(Seq(
      StructField("prediction", DoubleType, nullable = true),
      StructField("temperature", DoubleType, nullable = true),
      StructField("year", DoubleType, nullable = true),
      StructField("month", DoubleType, nullable = true),
      StructField("country", StringType, nullable = true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataArray), schema)
    df.show()

    // 关闭SparkSession
    spark.stop()
  }

  //正则替换
  def regexpReplace(df: DataFrame, columnName: String, regexp: String, newValue: Any): DataFrame = {
    val exprString: String = "regexp_replace(" + columnName + ",'" + regexp + "','" + newValue + "')"
    df.withColumn(columnName, expr(exprString).alias(columnName))
  }
}