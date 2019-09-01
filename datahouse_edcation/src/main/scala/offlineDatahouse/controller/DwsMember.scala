package offlineDatahouse.controller

import offlineDatahouse.service.DwsService
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DwsMember {

	def main(args: Array[String]): Unit = {
		System.setProperty("hadoop.home.dir", "C:\\Programs\\hadoop-2.7.2")
		System.setProperty("HADOOP_USER_NAME", "atguigu")
		val sparkConf: SparkConf = new SparkConf().setAppName("dws_member_import").setMaster("local[*]")
		val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
		val ssc: SparkContext = sparkSession.sparkContext

		HiveUtil.openDynamicPartition(sparkSession) //开启动态分区
		HiveUtil.openCompression(sparkSession) //开启压缩
		HiveUtil.useSnappyCompression(sparkSession) //使用snappy压缩

		val dt = "20190722"

		//学员信息宽表拉链表导入
		DwsService.importMember(sparkSession, dt)
		//章节维度表
		DwsService.saveDwsQzChapter(sparkSession, dt)
		//课程维度表
		DwsService.saveDwsQzCourse(sparkSession, dt)
		//主修维度表
		DwsService.saveDwsQzMajor(sparkSession, dt)
		//试卷维度表
		DwsService.saveDwsQzPaper(sparkSession, dt)
		//题目维度表
		DwsService.saveDwsQzQuestionTpe(sparkSession, dt)


	}

}