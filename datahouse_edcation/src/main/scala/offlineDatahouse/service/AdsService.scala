package offlineDatahouse.service

import offlineDatahouse.bean.userInfo.UserQueryResult
import offlineDatahouse.dao.user.DwsMemberDao
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, Row, SaveMode, SparkSession}

object AdsService {

	def MemberQueryDetail(sparkSession: SparkSession, dt: String) = {

		import sparkSession.implicits._

		val dataResourceDS: Dataset[UserQueryResult] = DwsMemberDao.queryIdlMemberData(sparkSession).as[UserQueryResult].where(s"dt = '${dt}'")

		dataResourceDS.cache()

		//统计通过各注册跳转地址(appregurl)进行注册的用户数
		/*select appregurl, count(uid) AS `count(uid)` , dn, dt
		  from dws.dws_member
		  group by appregurl, dn, dt
		*/
		val aggrTuple: Dataset[(String, Int)] = dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.appregurl + "_" + item.dn + "_" + item.dt, 1))
		})
		val groupBykeyDS: KeyValueGroupedDataset[String, (String, Int)] = aggrTuple.groupByKey(_._1)
		val mapValuesDS: KeyValueGroupedDataset[String, Int] = groupBykeyDS.mapValues((items: (String, Int)) => items._2)
		val reduceGroupDS: Dataset[(String, Int)] = mapValuesDS.reduceGroups(_ + _)

		val result: Dataset[(String, Int, String, String)] = reduceGroupDS.map(item => {
			val keys: Array[String] = item._1.split("_")

			val appregurl = keys(0)
			val dn = keys(1)
			val dt = keys(2)
			(appregurl, item._2, dt, dn)
		})

		result.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_appregurlnum")

		//		sparkSession.sql("select * from ads.ads_register_appregurlnum").show()


		//统计各所属网站（sitename）的用户数
		/*
			select sitename, count(uid) as `count(uid)`, dn, dt
			from dws.dws_member
			group by sitename, dn, dt
		 */

		//对每个分区的数据进行格式转化==> (item.sitename_itemdn_item.dt, 1)
		val mapDS: Dataset[(String, Int)] = dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.sitename + "_" + item.dn + "_" + item.dt, 1))
		})
		//根据key分组 ==> TODO ??
		val groupByKey: KeyValueGroupedDataset[String, (String, Int)] = mapDS.groupByKey(_._1)
		val tupleDS: Dataset[(String, Int)] = groupByKey.mapValues((item: (String, Int)) => item._2).reduceGroups(_ + _)
		val result2: Dataset[(String, Int, String, String)] = tupleDS.map(item => {
			val keys: Array[String] = item._1.split("_")

			val sitename = keys(0)
			val dn = keys(1)
			val dt = keys(2)
			(sitename, item._2, dt, dn)
		})

		result2.toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_sitenamenum")
		//		sparkSession.sql("select * from ads.ads_register_sitenamenum").show()


		//统计各所属平台的（regsourcename）用户数
		/*
			SELECT regsourcename, count(uid) AS `count(uid)`, dn, dt
			FROM dws.dws_member
			GROUP BY regsourcename, dn, dt;
		 */
		dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.regsourcename + "_" + item.dn + "_" + item.dt, 1))
		}).groupByKey((_: (String, Int))._1).mapValues(_._2).reduceGroups(_ + _).map(item => {
			val keys: Array[String] = item._1.split("_")

			val regsourcename = keys(0)
			val dn = keys(1)
			val dt = keys(2)
			(regsourcename, item._2, dt, dn)
		}).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_regsourcenamenum")

		//		sparkSession.sql("select * from ads.ads_register_regsourcenamenum").show()


		//需求7:统计通过各广告进来的人数
		/*
			SELECT adname, count(uid) AS `count(uid)`, dn, dt
			FROM dws.dws_member
			GROUP BY adname, dt, dn;
		 */
		dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.adname + "_" + item.dn + "_" + item.dt, 1))
		}).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
		  .map(item => {
			  val keys = item._1.split("_")
			  val adname = keys(0)
			  val dn = keys(1)
			  val dt = keys(2)
			  (adname, item._2, dn, dt)
		  }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_adnamenum")

		//		sparkSession.sql("select * from ads.ads_register_adnamenum").show()

		//需求8：统计各用户级别（memberlevel）的用户数
		/*
			SELECT memberlevel, count(uid) AS `count(uid)`, dn, dt
			FROM dws.dws_member
			GROUP BY memberlevel, dn, dt;
		 */
		dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.memberlevel + "_" + item.dn + "_" + item.dt, 1))
		}).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
		  .map(item => {

			  val keys = item._1.split("_")

			  val memberlevel = keys(0)
			  val dn = keys(1)
			  val dt = keys(2)
			  (memberlevel, item._2, dt, dn)
		  }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_memberlevelnum")
		//		sparkSession.sql("select * from ads.ads_register_memberlevelnum").show()

		//需求9：统计各vip等级人数
		/*
			SELECT vip_level, count(uid) AS `count(uid)`, dn, dt
			FROM dws.dws_member
			GROUP BY vip_level, dn, dt;
		 */
		dataResourceDS.mapPartitions(partition => {
			partition.map(item => (item.vip_level + "_" + item.dn + "_" + item.dt, 1))
		}).groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
		  .map(item => {
			  val keys = item._1.split("_")
			  val vip_level = keys(0)
			  val dn = keys(1)
			  val dt = keys(2)
			  (vip_level, item._2, dt, dn)
		  }).toDF().coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_viplevelnum")
		//		sparkSession.sql("select * from ads.ads_register_viplevelnum").show()


		//需求10：统计各分区网站、用户级别下(website、memberlevel)的top3用户
		/*
			SELECT *
			FROM (SELECT uid,
			             ad_id,
			             memberlevel,
			             register,
			             appregurl,
			             regsource,
			             regsourcename,
			             adname,
			             siteid,
			             sitename,
			             vip_level,
			             cast(paymoney AS decimal(10, 4)),
			             row_number() OVER
			                 (PARTITION BY dn,memberlevel ORDER BY cast(paymoney AS decimal(10, 4)) DESC) AS rownum,
			             dn
			      FROM dws.dws_member
			      WHERE dt = "20190722") r
			WHERE rownum < 4
			ORDER BY memberlevel, rownum
		 */
		//导入sql.functions以支持开窗函数row_number等函数
		import org.apache.spark.sql.functions._

		//withColumn:通过添加列或替换具有相同名称的现有列，返回新数据集
		dataResourceDS.withColumn("rownum", row_number().over(Window.partitionBy("dn", "memberlevel")
		  .orderBy(desc("paymoney"))))
		  .where("rownum < 4").orderBy("memberlevel", "rownum")
		  .select("uid", "memberlevel", "register", "appregurl", "regsourcename", "adname", "sitename",
			  "vip_level", "paymoney", "rownum", "dt", "dn")
		  .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_register_top3memberpay")

		//		sparkSession.sql("select * from ads.ads_register_top3memberpay limit 10").show()
	}

	//====================================================Quize=========================================================


	def QzQueryDetail(sparkSession: SparkSession, dt: String) = {

		import org.apache.spark.sql.functions._

		//基于宽表统计各试卷平均耗时、平均分
		/*
			SELECT paperviewid,
			       paperviewname,
			       cast(avg(score) AS decimal(4, 1))      avgscore,
			       cast(avg(spendtime) AS decimal(10, 2)) avgspendtime,
			       dn,
			       dt
			FROM dws.dws_user_paper_detail
			GROUP BY paperviewid, paperviewname, dn, dt
			WHERE dt = 20190722
			ORDER BY avgscore DESC;
		 */
		val avgDetail: DataFrame = sparkSession.sql("select paperviewid, paperviewname, score, spendtime, dt, dn from dws.dws_user_paper_detail")
		  .where(s"dt = ${dt}").groupBy("paperviewid", "paperviewname", "dt", "dn")
		  .agg(avg("score").cast("decimal(4, 1)").as("avgscore"),
			  avg("spendtime").cast("decimal(10, 2)").as("avgspendtime"))
		  .select("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")

		//		avgDetail.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_avgtimeandscore")
		//		avgDetail.show()
		//
		//		//println("=============avgDetail============")


		//统计各试卷最高分、最低分
		/*
			SELECT paperviewid,
			       paperviewname,
			       cast(max(score) AS decimal(4, 1)) maxscore,
			       cast(min(score) AS decimal(4, 1)) minscore,
			       dn,
			       dt
			FROM dws.dws_user_paper_detail
			WHERE dt = 20190722
			GROUP BY paperviewid, paperviewname, dn, dt;
		 */
		//读取全部数据
		val topScore: DataFrame = sparkSession.sql("select paperviewid, paperviewname, score, dt, dn from dws.dws_user_paper_detail")
		  //过滤条件
		  .where(s"dt = ${dt}").groupBy("paperviewid", "paperviewname", "dt", "dn")
		  //聚合函数
		  .agg(max("score").as("maxscore"), min("score").as("minscore"))
		  //输出字段
		  .select("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")

		//导入数据表
		//		topScore.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")
		//		topScore.show()
		//		//println("=============topScore============")

		//按试卷分组统计每份试卷的前三用户详情
		/*
			SELECT *
			FROM (
					 SELECT userid,
							paperviewid,
							paperviewname,
							chaptername,
							pointname,
							sitecoursename,
							coursename,
							majorname,
							shortname,
							score,
							dense_rank() OVER (PARTITION BY paperviewid ORDER BY score DESC ) rk,
							dt,
							dn
					 FROM dws.dws_user_paper_detail) tmp
			WHERE rk < 4
		 */
		//选取所有数据
		val top3UserDetail: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail")
		  //按日期过滤
		  .where(s"dt =${dt}")
		  //选择需要的字段
		  .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
			  , "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")
		  //增加一列
		  .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy(desc("score"))))
		  .where("rk < 4")
		  .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
			  , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")

		//数据导入
		//		top3UserDetail.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")
		//		top3UserDetail.show()

		//按试卷分组统计每份试卷的倒数前三的用户详情
		val low3UserDetail: DataFrame = sparkSession.sql("select *from dws.dws_user_paper_detail")

		  .where(s"dt=$dt").select("userid", "paperviewid", "paperviewname", "chaptername", "pointname"
			, "sitecoursename", "coursename", "majorname", "shortname", "papername", "score", "dt", "dn")

		  .withColumn("rk", dense_rank().over(Window.partitionBy("paperviewid").orderBy("score")))
		  .where("rk<4")
		  .select("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename"
			  , "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")

		//		low3UserDetail.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_low3_userdetail")
		//		low3UserDetail.show()

		//println("=============low3UserDetail============")


		//统计各试卷各分段的用户id，分段有0-20,20-40,40-60，60-80,80-100
		/*
			SELECT paperviewid,
				   paperviewname,
				   score_segment,
				   concat_ws(',', collect_list(cast(userid AS STRING))) userids,
				   dt,
				   dn
			FROM (SELECT paperviewid,
						 paperviewname,
						 userid,
						 CASE
							 WHEN score >= 0 AND score <= 20 THEN '0-20'
							 WHEN score >= 20 AND score <= 40 THEN '20-40'
							 WHEN score >= 40 AND score <= 60 THEN '40-60'
							 WHEN score >= 60 AND score <= 80 THEN '60-80'
							 WHEN score >= 80 AND score <= 100 THEN '80-100' END AS score_segment,
						 dt,
						 dn
				  FROM dws.dws_user_paper_detail
				  WHERE dt = 20190722) tmp
			GROUP BY paperviewid, paperviewname, score_segment, dt, dn
		 */
		val paperScore: Dataset[Row] = sparkSession.sql("select *from dws.dws_user_paper_detail")
		  .where(s"dt = ${dt}")
		  //选择提供必要数据的字段
		  .select("paperviewid", "paperviewname", "userid", "score", "dt", "dn")
		  //为分数段设置别名
		  .withColumn("score_segment",
			  //between :[0,20]
			  when(col("score").between(0, 20), "0-20")
				.when(col("score") > 20 && col("score") <= 40, "20-40")
				.when(col("score") > 40 && col("score") <= 60, "40-60")
				.when(col("score") > 60 && col("score") <= 80, "60-80")
				.when(col("score") > 80 && col("score") <= 100, "80-100"))
		  //score字段昨晚分数段处理就不再需要,drop
		  .drop("score").groupBy("paperviewid", "paperviewname", "score_segment", "dt", "dn")
		  .agg(concat_ws(",", collect_list("userid").cast("string")).as("userids"))
		  //TODO ??.as("userids")
		  .select("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
		  .orderBy("paperviewid", "score_segment")

		paperScore.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")
		paperScore.show()
		//println("=============paperScore============")


		//统计试卷未及格的人数，及格的人数，试卷的及格率 及格分数60
		/*
		SELECT t.*,
			   cast(t.passcount / (t.passcount + t.countdetail) AS decimal(4, 2)) AS rate,
			   dt,
			   dn
		FROM (
				 SELECT a.paperviewid, a.paperviewname, a.countdetail, a.dt, a.dn, b.passcount
				 FROM (
						  SELECT paperviewid, paperviewname, count(*) countdetail, dt, dn
						  FROM dws.dws_user_paper_detail
						  WHERE dt = '20190722'
							AND score BETWEEN 0 AND 59
						  GROUP BY paperviewid, paperviewname, dt, dn) a
						  JOIN
					  (
						  SELECT paperviewid, paperviewname, count(*) passcount, dn
						  FROM dws.dws_user_paper_detail
						  WHERE dt = '20190722'
							AND score BETWEEN 60 AND 100
						  GROUP BY paperviewid, paperviewname, dn
					  ) b
					  ON a.paperviewid = b.paperviewid AND a.dn = b.dn) t
		 */
		//获取全部需要的数据,并缓存(两次使用)
		val necessaryDataDF: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()
		//计算及格人数
		val unPass: DataFrame = necessaryDataDF.select("paperviewid", "paperviewname", "dn", "dt")
		  .where(s"dt = ${dt}").where("score between 0 and 59")
		  .groupBy("paperviewid", "paperviewname", "dn", "dt")
		  .agg(count("paperviewid").as("unpasscount"))
		//计算不及格人数
		val pass: DataFrame = necessaryDataDF.select("paperviewid", "dn")
		  .where(s"dt = ${dt}").where("score>=60")
		  .groupBy("paperviewid", "dn")
		  .agg(count("paperviewid").as("passcount"))

		val passRateDetail: DataFrame = unPass.join(pass, Seq("paperviewid", "dn"))
		  //求及格率
		  .withColumn("rate", col("passcount")./(col("passcount") + col("unpasscount"))
			.cast("decimal(4,2)"))
		  //输出结果
		  .select("paperviewid", "paperviewname", "unpasscount", "passcount", "rate", "dt", "dn")

		passRateDetail.coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

		//释放缓存
		necessaryDataDF.unpersist()
		passRateDetail.show()
		//println("=============passRateDetail============")


		//统计各题的错误数，正确数，错题率

		val userQuestionDetail: DataFrame = sparkSession.sql("select * from dws.dws_user_paper_detail").cache()

		val userQuestionError: DataFrame = userQuestionDetail.select("questionid", "dt", "dn", "user_question_answer")
		  //user_question_answer用于辅助统计错题数且不在最后结果输出,用完之后drop
		  .where(s"dt='$dt'").where("user_question_answer='0'").drop("user_question_answer")
		  .groupBy("questionid", "dt", "dn")
		  .agg(count("questionid").as("errcount"))

		val userQuestionRight = userQuestionDetail.select("questionid", "dn", "user_question_answer")
		  .where(s"dt='$dt'").where("user_question_answer='1'").drop("user_question_answer")
		  .groupBy("questionid", "dn")
		  .agg(count("questionid").as("rightcount"))

		val errorQuestionRate: DataFrame = userQuestionError.join(userQuestionRight, Seq("questionid", "dn"))
		  .withColumn("rate", (col("errcount") / (col("errcount") + col("rightcount"))).cast("decimal(4,2)"))
		  .orderBy(desc("errcount")).coalesce(1)
		  .select("questionid", "errcount", "rightcount", "rate", "dt", "dn")

		errorQuestionRate.write.mode(SaveMode.Append).insertInto("ads.ads_user_question_detail")
		errorQuestionRate.unpersist()
		errorQuestionRate.show()
		//println("=============errorQuestionRate============")

	}

}
