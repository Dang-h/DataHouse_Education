package offlineDatahouse.controller

import offlineDatahouse.service.DwsService
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwdMember {

	def main(args: Array[String]): Unit = {
		System.setProperty("hadoop.home.dir", "C:\\Programs\\hadoop-2.7.2")
		System.setProperty("HADOOP_USER_NAME", "atguigu")
		val sparkConf: SparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc: SparkContext = sparkSession.sparkContext

		HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
		HiveUtil.openCompression(sparkSession) //开启压缩
		HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

		DwsService.importMember(sparkSession, "20190722")
//		DwsService.testLeftJoinFirst(sparkSession,"20190722")

	}

}
