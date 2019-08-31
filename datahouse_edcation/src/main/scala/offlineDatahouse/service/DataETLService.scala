package offlineDatahouse.service

import com.alibaba.fastjson.JSONObject
import offlineDatahouse.utils.ParseJson
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataETLService {

	/**
	 * 敏感数据脱敏
	 * 如：用户名：王XX   手机号：137*****789  密码直接替换成******
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {

		//隐式转换,作用:为了调用rdd转dataStream方法——toDF
		import sparkSession.implicits._

		//过滤非JSON对象
		val filterJSONRDD: RDD[String] = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/member.log").filter(item => {
			val json: JSONObject = ParseJson.getJsonData(item)
			json.isInstanceOf[JSONObject]
		})

		//关键数据脱敏
		//mapPartitions作用:独立在RDD的每一个分区上进行RDD格式转换
		val mapRDD: RDD[(Int, Int, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = filterJSONRDD.mapPartitions(partition => {
			//数据脱敏，用户名：王XX   手机号：137*****789  密码直接替换成******
			partition.map(item => {
				val jsonObject: JSONObject = ParseJson.getJsonData(item)
				val uid: Int = jsonObject.getIntValue("uid")
				val ad_id: Int = jsonObject.getIntValue("ad_id")
				val birthday: String = jsonObject.getString("birthday")
				val email: String = jsonObject.getString("email")
				val fullname: String = jsonObject.getString("fullname").substring(0, 1) + "XX"
				val iconurl: String = jsonObject.getString("iconurl")
				val lastlogin: String = jsonObject.getString("lastlogin")
				val mailaddr: String = jsonObject.getString("mailaddr")
				val memberlevel: String = jsonObject.getString("memberlevel")
				val password = "******"
				val paymoney: String = jsonObject.getString("paymoney")
				val phone: String = jsonObject.getString("phone")
				val newphone: String = phone.substring(0, 3) + "*****" + phone.substring(7, 11)
				val qq: String = jsonObject.getString("qq")
				val register: String = jsonObject.getString("register")
				val regupdatetime: String = jsonObject.getString("regupdatetime")
				val unitname = jsonObject.getString("unitname")
				val userip: String = jsonObject.getString("userip")
				val zipcode: String = jsonObject.getString("zipcode")
				val dt: String = jsonObject.getString("dt")
				val dn: String = jsonObject.getString("dn")
				//字段未超过22个可以用元组存储
				(uid, ad_id, birthday, email, fullname, iconurl, lastlogin, mailaddr, memberlevel, password, paymoney, newphone, qq,
				  register, regupdatetime, unitname, userip, zipcode, dt, dn)
			})
		})

		//将数据追加（Append）写入指定表；insertInto不需要字段名对应，只需位置对应；它要求写入的表必须存在
		mapRDD.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
	}

}
