package offlineDatahouse.controller

import offlineDatahouse.service.DwdDataETLService
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
		DwdDataETLService.etlMemberLog(ssc, sparkSession)
		//		DwdDataETLService.etlMemberLog1(sparkSession)
		//广告基础表
		DwdDataETLService.etlBaseadLog(ssc, sparkSession)
		//网站基础表
		DwdDataETLService.etlBasewebsiteLog(ssc, sparkSession)
		//用户跳转地址注册表
		DwdDataETLService.etlMemberRegtypeLog(ssc, sparkSession)
		//用户支付金额表
		DwdDataETLService.etlMemPayMoneyLog(ssc, sparkSession)
		//用户等级表
		DwdDataETLService.etlMemVipLevelLog(ssc, sparkSession)

		//==============================================做题模块=======================================================//
		//做题网站日志
		DwdDataETLService.etlQzWebsite(ssc, sparkSession)
		//网站课程日志
		DwdDataETLService.etlQzSiteCourse(ssc, sparkSession)
		//题目类型数据
		DwdDataETLService.etlQzQuestionType(ssc, sparkSession)
		//做题日志
		DwdDataETLService.etlQzQuestion(ssc, sparkSession)
		//做题知识点关联数据
		DwdDataETLService.etlQzPointQuestion(ssc, sparkSession)
		//知识点
		DwdDataETLService.etlQzPoint(ssc, sparkSession)
		//试卷试图
		DwdDataETLService.etlQzPaperView(ssc, sparkSession)
		//试卷
		DwdDataETLService.etlQzPaper(ssc, sparkSession)
		//做题详情
		DwdDataETLService.etlQzMemberPaperQuestion(ssc, sparkSession)
		//主修
		DwdDataETLService.etlQzMajor(ssc, sparkSession)
		//课程辅导
		DwdDataETLService.etlQzCourseEdusubject(ssc, sparkSession)
		//题库
		DwdDataETLService.etlQzCourse(ssc, sparkSession)
		//章节列表
		DwdDataETLService.etlQzChapterList(ssc, sparkSession)
		//章节数据
		DwdDataETLService.etlQzChapter(ssc, sparkSession)
		//试卷主题关联
		DwdDataETLService.etlQzCenterPaper(ssc, sparkSession)
		//主题
		DwdDataETLService.etlQzCenter(ssc, sparkSession)
		//行业
		DwdDataETLService.etlQzBusiness(ssc, sparkSession)

	}

}
