package process

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import util.DBTools


object YearCountryDepressionNoProcess {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("YearCountryDepressionNoProcess").getOrCreate()

    //val inputWebfile = "input/depression.csv"
    val inputFile = args(0)
    var weatherData:DataFrame = sparkSession.read.format("csv").option("header",true).option("multLine",true).load(inputFile)
    //weatherData=regexpReplace()
    //weatherData.show()
    weatherData.createOrReplaceTempView("tbl")
    val sql =
      """
        |SELECT year, country, SUM(depressed_no) AS depressed_count
        |FROM tbl
        |GROUP BY year, country
        |ORDER BY year, country;
        | """.stripMargin
    var result = sparkSession.sql(sql)
    result.show(false)
    DBTools.WriteMySql("year_country_depression_no",result)
  }

}
