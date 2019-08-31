package offlineDatahouse.controller

import offlineDatahouse.service.DataETLService
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DwdController {
	def main(args: Array[String]): Unit = {
		System.setProperty("hadoop.home.dir", "C:\\Programs\\hadoop-2.7.2")
		System.setProperty("HADOOP_USER_NAME", "atguigu")
		val sparkConf: SparkConf = new SparkConf().setAppName("dwd_member_import").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc: SparkContext = sparkSession.sparkContext

		//hive优化
		HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
		HiveUtil.openCompression(sparkSession) //开启压缩
		HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

		//需求1：用户数据脱敏
		DataETLService.etlMemberLog(ssc, sparkSession)

		sparkSession.stop()
	}

}
