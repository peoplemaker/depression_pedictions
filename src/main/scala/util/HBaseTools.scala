package util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseTools {
  /**
   * 功能：获取连接Hbase的Configuration对象
   *
   * @return
   */
  def getHbaseConn(tableName: String): Configuration = {
    try {
      val hconf: Configuration = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.property.clientPort", "2181");
      /*hconf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");*/
      hconf.set("hbase.zookeeper.quorum", "192.168.88.128:2181");

      //设置读取HBase表的名称和读取数量
      hconf.set(TableInputFormat.INPUT_TABLE, tableName);
      hconf.set(TableInputFormat.SCAN_BATCHSIZE, "100")
      hconf
    } catch {
      case exception: Exception =>
        sys.error(exception.getMessage)
        sys.error("HBase连接失败！")
        null
    }
  }

}
