package offlineDatahouse.controller

import offlineDatahouse.service.DataETLService
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DwdDataImport {
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
		//		DataETLService.etlMemberLog1(sparkSession)
		//广告基础表
		DataETLService.etlBaseadLog(ssc, sparkSession)
		//网站基础表
		DataETLService.etlBasewebsiteLog(ssc, sparkSession)
		//用户跳转地址注册表
		DataETLService.etlMemberRegtypeLog(ssc, sparkSession)
		//用户支付金额表
		DataETLService.etlMemPayMoneyLog(ssc, sparkSession)
		//用户等级表
		DataETLService.etlMemVipLevelLog(ssc, sparkSession)

		//==============================================做题模块=======================================================//
		//做题网站日志
		DataETLService.etlQzWebsite(ssc, sparkSession)
		//网站课程日志
		DataETLService.etlQzSiteCourse(ssc, sparkSession)
		//题目类型数据
		DataETLService.etlQzQuestionType(ssc, sparkSession)
		//做题日志
		DataETLService.etlQzQuestion(ssc, sparkSession)
		//做题知识点关联数据
		DataETLService.etlQzPointQuestion(ssc, sparkSession)
		//知识点
		DataETLService.etlQzPoint(ssc, sparkSession)
		//试卷试图
		DataETLService.etlQzPaperView(ssc, sparkSession)
		//试卷
		DataETLService.etlQzPaper(ssc, sparkSession)
		//做题详情
		DataETLService.etlQzMemberPaperQuestion(ssc, sparkSession)
		//主修
		DataETLService.etlQzMajor(ssc, sparkSession)
		//课程辅导
		DataETLService.etlQzCourseEdusubject(ssc, sparkSession)
		//题库
		DataETLService.etlQzCourse(ssc, sparkSession)
		//章节列表
		DataETLService.etlQzChapterList(ssc, sparkSession)
		//章节数据
		DataETLService.etlQzChapter(ssc, sparkSession)
		//试卷主题关联
		DataETLService.etlQzCenterPaper(ssc, sparkSession)
		//主题
		DataETLService.etlQzCenter(ssc, sparkSession)
		//行业
		DataETLService.etlQzBusiness(ssc, sparkSession)

		sparkSession.stop()
	}

}
