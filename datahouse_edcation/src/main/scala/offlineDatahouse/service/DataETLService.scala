package offlineDatahouse.service

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import offlineDatahouse.bean.MemberLog
import offlineDatahouse.utils.ParseJson


/*
需求1：必须使用Spark进行数据清洗，对用户名、手机号、密码进行脱敏处理，并使用Spark将数据导入到dwd层hive表中
清洗规则 用户名：王XX   手机号：137*****789  密码直接替换成******
 */
object DataETLService {

  def etlMemberLog(ssc:SparkContext, sparkSession:SparkSession)={
	//隐式转换,TODO 作用:
	import sparkSession.implicits._

	//过滤非JSON对象
	val filterJSONRDD: RDD[String] = ssc.textFile("/user/atguigu/ods/member.log").filter(item => {
	  val json: JSONObject = ParseJson.getJsonData(item)
	  json.isInstanceOf[JSONObject]
	})

	//mapPartitions作用:独立在RDD的每一个分区上进行RDD格式转换
	filterJSONRDD.mapPartitions(partition =>{
	  partition.map(item =>{
		val jsonObject: JSONObject = ParseJson.getJsonData(item)
		val uid: Int = jsonObject.getIntValue("uid")
		val ad_id: Int = jsonObject.getIntValue("ad_id")
		val birthday: String = jsonObject.getString("birthday")
		val email: String = jsonObject.getString("email")
		val fullname: String = jsonObject.getString("fullname")

	  })
	})

  }


  def main(args: Array[String]): Unit = {
	val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UserInfoETL")
	val sc = new SparkContext(sparkConf)

	val session: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
	val ssc: SparkContext = session.sparkContext


	val linesRDD: RDD[String] = sc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/member.log")

	val etlRDD: RDD[MemberLog] = linesRDD.map(line => {
	  //清洗规则 用户名：王XX   手机号：137*****789  密码直接替换成******

	  val userInfo: MemberLog = JSON.parseObject(line, classOf[MemberLog])

	  val nameTuple: (String, String) = userInfo.fullname.splitAt(1)
	  userInfo.fullname = nameTuple._1 + "XX"

	  val phoneTuple: (String, String) = userInfo.phone.splitAt(3)
	  userInfo.phone = phoneTuple._1 + "*****" + phoneTuple._2.splitAt(5)._2

	  userInfo.password = "******"

	  userInfo
	})

	import session.implicits._
	etlRDD.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")

  }

}
