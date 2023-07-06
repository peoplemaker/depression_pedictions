package util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DBTools {

  def getSession(appname:String,master:String) ={
    val session = SparkSession.builder().master(master).appName(appname).getOrCreate()
    session
  }

  def WriteMySql(tableName:String,result:DataFrame) ={
    result.write
      .format("jdbc")
      .option("url", "jdbc:mysql://guet.gxist.cn:3306/depression_db")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "562mmNRTntGrzF8C")
      .option("dbtable",tableName)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
