package process

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.DBTools


object YearCountryDepressedPerProcess {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("YearCountryDepressedPerProcess").getOrCreate()

    val inputWebfile = "input/depression.csv"
    //val inputFile = args(0)
    var weatherData:DataFrame = sparkSession.read.format("csv").option("header",true).option("multLine",true).load(inputWebfile)
    //weatherData=regexpReplace()
    //weatherData.show()
    weatherData.createOrReplaceTempView("tbl")
    val sql =
      """
        |SELECT country, year, depressed_per_hundred_k_pop,log(CAST(REPLACE(gdp_for_year, ',', '') AS DOUBLE)) AS log_gdp
        |FROM tbl;
        | """.stripMargin
    var result = sparkSession.sql(sql)
    result.createOrReplaceTempView("tbl1")
    val sql2 =
      """
        |SELECT
        |  CASE
        |    WHEN log_gdp >= 20 AND log_gdp < 21 THEN '20'
        |    WHEN log_gdp >= 21 AND log_gdp < 22 THEN '21'
        |    WHEN log_gdp >= 22 AND log_gdp < 23 THEN '22'
        |    WHEN log_gdp >= 23 AND log_gdp < 24 THEN '23'
        |    WHEN log_gdp >= 24 AND log_gdp < 25 THEN '24'
        |    WHEN log_gdp >= 25 AND log_gdp < 26 THEN '25'
        |    WHEN log_gdp >= 26 AND log_gdp < 27 THEN '26'
        |    WHEN log_gdp >= 27 AND log_gdp < 28 THEN '27'
        |    WHEN log_gdp >= 28 AND log_gdp < 29 THEN '28'
        |    WHEN log_gdp >= 29 AND log_gdp < 30 THEN '29'
        |    ELSE '19'
        |  END AS group,
        |  country,
        |  year,
        |  sum(depressed_per_hundred_k_pop) AS sum_depressed_per_hundred_k_pop
        |FROM
        |  tbl1
        |WHERE
        |  year >= 1985 AND year <= 2014
        |GROUP BY
        |  group, country, year
        |""".stripMargin

    val summedResult = sparkSession.sql(sql2)
    val finalResult = summedResult.groupBy("group", "country").agg(avg("sum_depressed_per_hundred_k_pop").as("avg_depressedRate"))
    finalResult.show(false)
    DBTools.WriteMySql("gdp_scales_depressed_per",finalResult)

  }

}
