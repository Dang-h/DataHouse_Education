package offlineDatahouse.service

import com.alibaba.fastjson.{JSONObject, JSON}
import offlineDatahouse.bean.{Member, QzPaperView, QzPoint, QzQuestion}
import offlineDatahouse.utils.ParseJson
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}


object DataETLService {

	/**
	 * 敏感数据脱敏
	 * 如：用户名：王XX   手机号：137*****789  密码直接替换成******
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemberLog(ssc: SparkContext, sparkSession: SparkSession) = {

		//隐式转换,作用:为了调用rdd转dataFrame方法——toDF
		import sparkSession.implicits._

		//过滤非JSON对象
		val filterJSONRDD: RDD[String] = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/member.log").filter(item => {
			val json: JSONObject = ParseJson.getJsonData(item)
			json.isInstanceOf[JSONObject]
		})
		//		val filterJSONRDD: RDD[String] = ssc.textFile("D:\\DataHouse_Education\\datahouse_edcation\\input\\member.log").filter(item => {
		//			val json: JSONObject = ParseJson.getJsonData(item)
		//			json.isInstanceOf[JSONObject]
		//		})

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
		//		mapRDD.toDF().show()

		//将数据追加（Append）写入指定表；insertInto不需要字段名对应，只需位置对应；它要求写入的表必须存在
		mapRDD.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
	}

	//法二
	def etlMemberLog1(sparkSession: SparkSession) = {
		import sparkSession.implicits._
		//		val logDataFrame: DataFrame = sparkSession.read.json("hdfs://hadoop102:9000/user/atguigu/ods/member.log")
		//如果读入数据中有错误格式JSON会报错,无法对数据进行过滤
		val logDataFrame: DataFrame = sparkSession.read.json("D:\\DataHouse_Education\\datahouse_edcation\\input\\member.log")

		//DataFrame 转DataSet
		val logDataSet: Dataset[Member] = logDataFrame.as[Member]
		val etlLogDataSet: Dataset[Member] = logDataSet.map(ele => {
			ele.fullname = ele.fullname.charAt(0) + "XX"
			ele.phone = ele.phone.splitAt(3)._1 + "*****" + ele.phone.splitAt(8)._2
			ele.password = "******"
			ele
		})
		etlLogDataSet.show()
		//DataSet转DataFrame
		etlLogDataSet.toDF().coalesce(2).write.mode(SaveMode.Append).insertInto("dwd.dwd_member")
	}

	/**
	 * 导入广告基础数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlBaseadLog(ssc: SparkContext, sparkSession: SparkSession) {
		import sparkSession.implicits._

		val result: Unit = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baseadlog.log").filter(item => {
			val jSONObject: JSONObject = ParseJson.getJsonData(item)
			jSONObject.isInstanceOf[JSONObject]
		}).mapPartitions(partition => {
			partition.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val adid = jsonObject.getIntValue("adid")
				val adname = jsonObject.getString("adname")
				val dn = jsonObject.getString("dn")
				(adid, adname, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_ad")

		//		sparkSession.sql("select * from dwd.dwd_base_ad").show()
	}


	/**
	 * 网站基础表
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlBasewebsiteLog(ssc: SparkContext, sparkSession: SparkSession): Unit = {
		import sparkSession.implicits._

		val result: Unit = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/baswewebsite.log").filter(item => {
			val jSONObject: JSONObject = ParseJson.getJsonData(item)
			jSONObject.isInstanceOf[JSONObject]
		}).mapPartitions(partition => {
			partition.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val siteid = jsonObject.getIntValue("siteid")
				val sitename = jsonObject.getString("sitename")
				val siteurl = jsonObject.getString("siteurl")
				val delete = jsonObject.getIntValue("delete")
				val createtime = jsonObject.getString("createtime")
				val creator = jsonObject.getString("creator")
				val dn = jsonObject.getString("dn")
				(siteid, sitename, siteurl, delete, createtime, creator, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_base_website")

		//		sparkSession.sql("select * from dwd.dwd_base_website limit 10").show()
	}

	/**
	 * 用户跳转注册地址表
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemberRegtypeLog(ssc: SparkContext, sparkSession: SparkSession) = {

		import sparkSession.implicits._ //隐式转换

		val result = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/memberRegtype.log")
		  .filter(item => {
			  val obj = ParseJson.getJsonData(item)
			  obj.isInstanceOf[JSONObject]
		  }).mapPartitions(partitoin => {
			partitoin.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val appkey = jsonObject.getString("appkey")
				val appregurl = jsonObject.getString("appregurl")
				val bdp_uuid = jsonObject.getString("bdp_uuid")
				val createtime = jsonObject.getString("createtime")
				val domain = jsonObject.getString("webA")
				val isranreg = jsonObject.getString("isranreg")
				val regsource = jsonObject.getString("regsource")
				val regsourceName = regsource match {
					//"regsource": "4", //所属平台 1.PC  2.MOBILE  3.APP   4.WECHAT
					case "1" => "PC"
					case "2" => "Mobile"
					case "3" => "App"
					case "4" => "WeChat"
					case _ => "other"
				}
				val uid = jsonObject.getIntValue("uid")
				val websiteid = jsonObject.getIntValue("websiteid")
				val dt = jsonObject.getString("dt") //日期分区
				val dn = jsonObject.getString("dn") //网站分区
				(uid, appkey, appregurl, bdp_uuid, createtime, domain, isranreg, regsource, regsourceName, websiteid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_member_regtype")

		//		sparkSession.sql("select * from dwd.dwd_member_regtype limit 10").show()
	}


	/**
	 * 用户付款信息
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemPayMoneyLog(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._ //隐式转换
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcentermempaymoney.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partition => {
			partition.map(item => {
				val jSONObject = ParseJson.getJsonData(item)
				val paymoney = jSONObject.getString("paymoney")
				val uid = jSONObject.getIntValue("uid")
				val vip_id = jSONObject.getIntValue("vip_id")
				val site_id = jSONObject.getIntValue("siteid")
				val dt = jSONObject.getString("dt")
				val dn = jSONObject.getString("dn")
				(uid, paymoney, site_id, vip_id, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_pcentermempaymoney")

		//		sparkSession.sql("select * from dwd.dwd_pcentermempaymoney limit 10").show()
	}

	/**
	 * 用户vip基础数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlMemVipLevelLog(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._ //隐式转换
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/pcenterMemViplevel.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partition => {
			partition.map(item => {

				val jSONObject = ParseJson.getJsonData(item)
				val discountval = jSONObject.getString("discountval")
				val end_time = jSONObject.getString("end_time")
				val last_modify_time = jSONObject.getString("last_modify_time")
				val max_free = jSONObject.getString("max_free")
				val min_free = jSONObject.getString("min_free")
				val next_level = jSONObject.getString("next_level")
				val operator = jSONObject.getString("operator")
				val start_time = jSONObject.getString("start_time")
				val vip_id = jSONObject.getIntValue("vip_id")
				val vip_level = jSONObject.getString("vip_level")
				val dn = jSONObject.getString("dn")
				(vip_id, vip_level, start_time, end_time, last_modify_time, max_free, min_free, next_level, operator, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_vip_level")

		//		sparkSession.sql("select * from dwd.dwd_vip_level limit 10").show()
	}

	//==============================================做题模块============================================================//

	/**
	 * 做题网站数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzWebsite(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzWebsite.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val siteid = jsonObject.getIntValue("siteid")
				val sitename = jsonObject.getString("sitename")
				val domain = jsonObject.getString("domain")
				val sequence = jsonObject.getString("sequence")
				val multicastserver = jsonObject.getString("multicastserver")
				val templateserver = jsonObject.getString("templateserver")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val multicastgateway = jsonObject.getString("multicastgateway")
				val multicastport = jsonObject.getString("multicastport")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(siteid, sitename, domain, sequence, multicastserver, templateserver, status, creator, createtime,
				  multicastgateway, multicastport, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_website")
	}


	/**
	 * 网站课程
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzSiteCourse(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzSiteCourse.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val sitecourseid = jsonObject.getIntValue("sitecourseid")
				val siteid = jsonObject.getIntValue("siteid")
				val courseid = jsonObject.getIntValue("courseid")
				val sitecoursename = jsonObject.getString("sitecoursename")
				val coursechapter = jsonObject.getString("coursechapter")
				val sequence = jsonObject.getString("sequence")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val helppaperstatus = jsonObject.getString("helppaperstatus")
				val servertype = jsonObject.getString("servertype")
				val boardid = jsonObject.getIntValue("boardid")
				val showstatus = jsonObject.getString("showstatus")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(sitecourseid, siteid, courseid, sitecoursename, coursechapter, sequence, status, creator
				  , createtime, helppaperstatus, servertype, boardid, showstatus, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_site_course")
	}


	/**
	 * 题目类型
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzQuestionType(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzQuestionType.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val quesviewtype = jsonObject.getIntValue("quesviewtype")
				val viewtypename = jsonObject.getString("viewtypename")
				val questiontypeid = jsonObject.getIntValue("questypeid")
				val description = jsonObject.getString("description")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val papertypename = jsonObject.getString("papertypename")
				val sequence = jsonObject.getString("sequence")
				val remark = jsonObject.getString("remark")
				val splitscoretype = jsonObject.getString("splitscoretype")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(quesviewtype, viewtypename, questiontypeid, description, status, creator, createtime, papertypename, sequence,
				  remark, splitscoretype, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question_type")
	}


	/**
	 * 做题
	 *
	 * @param ssc
	 * @param sparkSession
	 * @return
	 */
	def etlQzQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzQuestion.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val questionid = jsonObject.getIntValue("questionid")
				val parentid = jsonObject.getIntValue("parentid")
				val questypeid = jsonObject.getIntValue("questypeid")
				val quesviewtype = jsonObject.getIntValue("quesviewtype")
				val content = jsonObject.getString("content")
				val answer = jsonObject.getString("answer")
				val analysis = jsonObject.getString("analysis")
				val limitminute = jsonObject.getString("limitminute")
				//对分数保留1位小数
				val score = BigDecimal.apply(jsonObject.getDoubleValue("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
				val splitscore = BigDecimal.apply(jsonObject.getDoubleValue("splitscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
				val status = jsonObject.getString("status")
				val optnum = jsonObject.getIntValue("optnum")
				val lecture = jsonObject.getString("lecture")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val modifystatus = jsonObject.getString("modifystatus")
				val attanswer = jsonObject.getString("attanswer")
				val questag = jsonObject.getString("questag")
				val vanalysisaddr = jsonObject.getString("vanalysisaddr")
				val difficulty = jsonObject.getString("difficulty")
				val quesskill = jsonObject.getString("quesskill")
				val vdeoaddr = jsonObject.getString("vdeoaddr")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				QzQuestion(questionid, parentid, questypeid, quesviewtype, content, answer, analysis, limitminute, score, splitscore,
					status, optnum, lecture, creator, createtime, modifystatus, attanswer, questag, vanalysisaddr, difficulty, quesskill,
					vdeoaddr, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_question")
	}


	/**
	 * 做题知识点关联数据
	 *
	 * @param ssc
	 * @param sparkSession
	 * @return
	 */
	def etlQzPointQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPointQuestion.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val pointid = jsonObject.getIntValue("pointid")
				val questionid = jsonObject.getIntValue("questionid")
				val questtype = jsonObject.getIntValue("questtype")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(pointid, questionid, questtype, creator, createtime, dt, dn)
			})
		}).toDF().write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point_question")
	}


	/**
	 * 知识点数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzPoint(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._

		val filterRDD: RDD[String] = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPoint.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		})

				filterRDD.mapPartitions(partitions => {
					partitions.map(item => {
						val jsonObject = ParseJson.getJsonData(item)
						val pointid = jsonObject.getIntValue("pointid")
						val courseid = jsonObject.getIntValue("courseid")
						val pointname = jsonObject.getString("pointname")
						val pointyear = jsonObject.getString("pointyear")
						val chapter = jsonObject.getString("chapter")
						val creator = jsonObject.getString("creator")
						val createtime = jsonObject.getString("createtime")
						val status = jsonObject.getString("status")
						val modifystatus = jsonObject.getString("modifystatus")
						val excisenum = jsonObject.getIntValue("excisenum")
						val pointlistid = jsonObject.getIntValue("pointlistid")
						val chapterid = jsonObject.getIntValue("chapterid")
						val sequence = jsonObject.getString("sequence")
						val pointdescribe = jsonObject.getString("pointdescribe")
						val pointlevel = jsonObject.getString("pointlevel")
						val typeslist = jsonObject.getString("typelist")
						//"score": 83.86880766562163,  //知识点分数
						//保留1位小数 并四舍五入
						val score = BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
						val thought = jsonObject.getString("thought")
						val remid = jsonObject.getString("remid")
						val pointnamelist = jsonObject.getString("pointnamelist")
						val typelistids = jsonObject.getString("typelistids")
						val pointlist = jsonObject.getString("pointlist")
						val dt = jsonObject.getString("dt")
						val dn = jsonObject.getString("dn")
						QzPoint(pointid, courseid, pointname, pointyear, chapter, creator, createtime, status, modifystatus, excisenum, pointlistid,
							chapterid, sequence, pointdescribe, pointlevel, typeslist, score, thought, remid, pointnamelist, typelistids,
							pointlist, dt, dn)
					})
				}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_point")


//		//法二,解析JSON
//		val etlData: RDD[QzPaperView] = filterRDD.mapPartitions(partition => {
//			partition.map(item => {
//				val jsonObject: JSONObject = JSON.parseObject(item)
//				jsonObject.put("score", BigDecimal(jsonObject.getDouble("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP))
//				jsonObject.toJavaObject(classOf[QzPoint])
//			})
//		})
//		etlData.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dwd.dwd_qz_point")
//		sparkSession.sql("select * from dwd.dwd_qz_paper_view limit 10").show(false)
	}


	/**
	 * 试卷试图
	 *
	 * @param ssc
	 * @param sparkSession
	 * @return
	 */
	def etlQzPaperView(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._

		val filterJsonRDD: RDD[String] = ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPaperView.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		})

		val etlJsonRDD: RDD[QzPaperView] = filterJsonRDD.mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val paperviewid = jsonObject.getIntValue("paperviewid")
				val paperid = jsonObject.getIntValue("paperid")
				val paperviewname = jsonObject.getString("paperviewname")
				val paperparam = jsonObject.getString("paperparam")
				val openstatus = jsonObject.getString("openstatus")
				val explainurl = jsonObject.getString("explainurl")
				val iscontest = jsonObject.getString("iscontest")
				val contesttime = jsonObject.getString("contesttime")
				val conteststarttime = jsonObject.getString("conteststarttime")
				val contestendtime = jsonObject.getString("contestendtime")
				val contesttimelimit = jsonObject.getString("contesttimelimit")
				val dayiid = jsonObject.getIntValue("dayiid")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val paperviewcatid = jsonObject.getIntValue("paperviewcatid")
				val modifystatus = jsonObject.getString("modifystatus")
				val description = jsonObject.getString("description")
				val papertype = jsonObject.getString("papertype")
				val downurl = jsonObject.getString("downurl")
				val paperuse = jsonObject.getString("paperuse")
				val paperdifficult = jsonObject.getString("paperdifficult")
				val testreport = jsonObject.getString("testreport")
				val paperuseshow = jsonObject.getString("paperuseshow")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				QzPaperView(paperviewid, paperid, paperviewname, paperparam, openstatus, explainurl, iscontest, contesttime,
					conteststarttime, contestendtime, contesttimelimit, dayiid, status, creator, createtime, paperviewcatid, modifystatus,
					description, papertype, downurl, paperuse, paperdifficult, testreport, paperuseshow, dt, dn)
			})
		})
		etlJsonRDD.toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper_view")

	}


	/**
	 * 做题日志
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzPaper(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzPaper.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val paperid = jsonObject.getIntValue("paperid")
				val papercatid = jsonObject.getIntValue("papercatid")
				val courseid = jsonObject.getIntValue("courseid")
				val paperyear = jsonObject.getString("paperyear")
				val chapter = jsonObject.getString("chapter")
				val suitnum = jsonObject.getString("suitnum")
				val papername = jsonObject.getString("papername")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val craetetime = jsonObject.getString("createtime")
				val totalscore = BigDecimal.apply(jsonObject.getString("totalscore")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
				val chapterid = jsonObject.getIntValue("chapterid")
				val chapterlistid = jsonObject.getIntValue("chapterlistid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(paperid, papercatid, courseid, paperyear, chapter, suitnum, papername, status, creator, craetetime, totalscore, chapterid,
				  chapterlistid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_paper")
	}


	/**
	 * 学院做题详情
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzMemberPaperQuestion(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzMemberPaperQuestion.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val userid = jsonObject.getIntValue("userid")
				val paperviewid = jsonObject.getIntValue("paperviewid")
				val chapterid = jsonObject.getIntValue("chapterid")
				val sitecourseid = jsonObject.getIntValue("sitecourseid")
				val questionid = jsonObject.getIntValue("questionid")
				val majorid = jsonObject.getIntValue("majorid")
				val useranswer = jsonObject.getString("useranswer")
				val istrue = jsonObject.getString("istrue")
				val lasttime = jsonObject.getString("lasttime")
				val opertype = jsonObject.getString("opertype")
				val paperid = jsonObject.getIntValue("paperid")
				val spendtime = jsonObject.getIntValue("spendtime")
				val score = BigDecimal.apply(jsonObject.getString("score")).setScale(1, BigDecimal.RoundingMode.HALF_UP)
				val question_answer = jsonObject.getIntValue("question_answer")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(userid, paperviewid, chapterid, sitecourseid, questionid, majorid, useranswer, istrue, lasttime, opertype, paperid, spendtime, score, question_answer, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_member_paper_question")
	}


	/**
	 * 主修数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzMajor(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzMajor.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val majorid = jsonObject.getIntValue("majorid")
				val businessid = jsonObject.getIntValue("businessid")
				val siteid = jsonObject.getIntValue("siteid")
				val majorname = jsonObject.getString("majorname")
				val shortname = jsonObject.getString("shortname")
				val status = jsonObject.getString("status")
				val sequence = jsonObject.getString("sequence")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val columm_sitetype = jsonObject.getString("columm_sitetype")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(majorid, businessid, siteid, majorname, shortname, status, sequence, creator, createtime, columm_sitetype, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_major")
	}


	/**
	 * 课程辅导数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzCourseEdusubject(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCourseEduSubject.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val courseeduid = jsonObject.getIntValue("courseeduid")
				val edusubjectid = jsonObject.getIntValue("edusubjectid")
				val courseid = jsonObject.getIntValue("courseid")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val majorid = jsonObject.getIntValue("majorid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(courseeduid, edusubjectid, courseid, creator, createtime, majorid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course_edusubject")
	}


	/**
	 * 题库课程
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzCourse(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCourse.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val courseid = jsonObject.getIntValue("courseid")
				val majorid = jsonObject.getIntValue("majorid")
				val coursename = jsonObject.getString("coursename")
				val coursechapter = jsonObject.getString("coursechapter")
				val sequence = jsonObject.getString("sequnece")
				val isadvc = jsonObject.getString("isadvc")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val status = jsonObject.getString("status")
				val chapterlistid = jsonObject.getIntValue("chapterlistid")
				val pointlistid = jsonObject.getIntValue("pointlistid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(courseid, majorid, coursename, coursechapter, sequence, isadvc, creator, createtime, status
				  , chapterlistid, pointlistid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_course")
	}


	/**
	 * 解析章节列表数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzChapterList(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzChapterList.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val chapterlistid = jsonObject.getIntValue("chapterlistid")
				val chapterlistname = jsonObject.getString("chapterlistname")
				val courseid = jsonObject.getIntValue("courseid")
				val chapterallnum = jsonObject.getIntValue("chapterallnum")
				val sequence = jsonObject.getString("sequence")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(chapterlistid, chapterlistname, courseid, chapterallnum, sequence, status, creator, createtime, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter_list")
	}


	/**
	 * 解析章节数据
	 *
	 * @param ssc
	 * @param sparkSession
	 * @return
	 */
	def etlQzChapter(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._ //隐式转换
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzChapter.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val chapterid = jsonObject.getIntValue("chapterid")
				val chapterlistid = jsonObject.getIntValue("chapterlistid")
				val chaptername = jsonObject.getString("chaptername")
				val sequence = jsonObject.getString("sequence")
				val showstatus = jsonObject.getString("showstatus")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val courseid = jsonObject.getIntValue("courseid")
				val chapternum = jsonObject.getIntValue("chapternum")
				val outchapterid = jsonObject.getIntValue("outchapterid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(chapterid, chapterlistid, chaptername, sequence, showstatus, status, creator, createtime,
				  courseid, chapternum, outchapterid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_chapter")
	}


	/**
	 * 试卷主题关联数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzCenterPaper(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCenterPaper.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val paperviewid = jsonObject.getIntValue("paperviewid") //试图id
				val centerid = jsonObject.getIntValue("centerid") //主题id
				val openstatus = jsonObject.getString("openstatus")
				val sequence = jsonObject.getString("sequence")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(paperviewid, centerid, openstatus, sequence, creator, createtime, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center_paper")
	}


	/**
	 * 主题数据
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzCenter(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzCenter.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(parititons => {
			parititons.map(item => {
				val jsonObject = ParseJson.getJsonData(item)
				val centerid = jsonObject.getIntValue("centerid")
				val centername = jsonObject.getString("centername")
				val centeryear = jsonObject.getString("centeryear")
				val centertype = jsonObject.getString("centertype")
				val openstatus = jsonObject.getString("openstatus")
				val centerparam = jsonObject.getString("centerparam")
				val description = jsonObject.getString("description")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val sequence = jsonObject.getString("sequence")
				val provideuser = jsonObject.getString("provideuser")
				val centerviewtype = jsonObject.getString("centerviewtype")
				val stage = jsonObject.getString("stage")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(centerid, centername, centeryear, centertype, openstatus, centerparam, description, creator, createtime,
				  sequence, provideuser, centerviewtype, stage, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_center")
	}


	/**
	 * 所属行业数据
	 * 成人教育,做题的人针对各自领域刷题;如:金融,互联网等
	 *
	 * @param ssc
	 * @param sparkSession
	 */
	def etlQzBusiness(ssc: SparkContext, sparkSession: SparkSession) = {
		import sparkSession.implicits._
		ssc.textFile("hdfs://hadoop102:9000/user/atguigu/ods/QzBusiness.log").filter(item => {
			val obj = ParseJson.getJsonData(item)
			obj.isInstanceOf[JSONObject]
		}).mapPartitions(partitions => {
			partitions.map(item => {
				val jsonObject = ParseJson.getJsonData(item);
				val businessid = jsonObject.getIntValue("businessid")
				val businessname = jsonObject.getString("businessname")
				val sequence = jsonObject.getString("sequence")
				val status = jsonObject.getString("status")
				val creator = jsonObject.getString("creator")
				val createtime = jsonObject.getString("createtime")
				val siteid = jsonObject.getIntValue("siteid")
				val dt = jsonObject.getString("dt")
				val dn = jsonObject.getString("dn")
				(businessid, businessname, sequence, status, creator, createtime, siteid, dt, dn)
			})
		}).toDF().coalesce(1).write.mode(SaveMode.Append).insertInto("dwd.dwd_qz_business")
	}


}
