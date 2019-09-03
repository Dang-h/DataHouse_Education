package offlineDatahouse.controller

import offlineDatahouse.service.AdsService
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object AdsController {

	def main(args: Array[String]): Unit = {
//		System.setProperty("hadoop.home.dir", "C:\\Programs\\hadoop-2.7.2")
//		System.setProperty("HADOOP_USER_NAME", "atguigu")
		val sparkConf: SparkConf = new SparkConf().setAppName("dwd_member_import")//.setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc: SparkContext = sparkSession.sparkContext

		HiveUtil.openDynamicPartition(sparkSession) //开启动态分区

		val dt = "20190722"

		//AdsMember
//		AdsService.MemberQueryDetail(sparkSession, dt)

		//AdsQuiz
		AdsService.QzQueryDetail(sparkSession, dt)

//		sparkSession.stop()

	}

}
