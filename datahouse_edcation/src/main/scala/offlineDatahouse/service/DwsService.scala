package offlineDatahouse.service

import offlineDatahouse.bean.userInfo.{DwsMember, DwsMember_Result, MemberZipper, MemberZipperResult}
import offlineDatahouse.dao.qz._
import offlineDatahouse.dao.user.DwdMemberDao
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  *@ClassName DwsService$
  *@Description //TODO 对dwd成数据进行轻度聚合降维以供ads层使用
  *
  *@Author Dang-h
  *@Email 54danghao@gmail.com
  *@Date 9:21 2019-9-2 0002
  *@Version 1.0
  *
 **/
object DwsService {

	/**
	 * 将6张基础表合并成一张宽表,并根据宽表支付金额和vip等级生成拉链表
	 *
	 * @param sparkSession
	 * @param time
	 */
	def importMember(sparkSession: SparkSession, time: String) = {
		import sparkSession.implicits._

		//查询全量数据 刷新到 TODO 宽表
		//宽表，以用户信息(dwd_member)为主表，在主表基础上并入其他明细表字段
		//以uid（用户id）和dn（网站分区）为条件用主表left join 其他表，再根据日期来过滤得到数据
		//s的作用：在字符串上下文的相应部分之间插入参数。
		val memberWideTable: DataFrame = sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
		  "first(mailaddr),first(memberlevel),first(password),sum(cast(paymoney as decimal(10,4))),first(phone),first(qq)," +
		  "first(register),first(regupdatetime),first(unitname),first(userip),first(zipcode)," +
		  "first(appkey),first(appregurl),first(bdp_uuid),first(reg_createtime),first(domain)," +
		  "first(isranreg),first(regsource),first(regsourcename),first(adname),first(siteid),first(sitename)," +
		  "first(siteurl),first(site_delete),first(site_createtime),first(site_creator),first(vip_id),max(vip_level)," +
		  "min(vip_start_time),max(vip_end_time),max(vip_last_modify_time),first(vip_max_free),first(vip_min_free),max(vip_next_level)," +
		  "first(vip_operator),dt,dn from" +
		  "(select a.uid,a.ad_id,a.fullname,a.iconurl,a.lastlogin,a.mailaddr,a.memberlevel," +
		  "a.password,e.paymoney,a.phone,a.qq,a.register,a.regupdatetime,a.unitname,a.userip," +
		  "a.zipcode,a.dt,b.appkey,b.appregurl,b.bdp_uuid,b.createtime as reg_createtime,b.domain,b.isranreg,b.regsource," +
		  "b.regsourcename,c.adname,d.siteid,d.sitename,d.siteurl,d.delete as site_delete,d.createtime as site_createtime," +
		  "d.creator as site_creator,f.vip_id,f.vip_level,f.start_time as vip_start_time,f.end_time as vip_end_time," +
		  "f.last_modify_time as vip_last_modify_time,f.max_free as vip_max_free,f.min_free as vip_min_free," +
		  "f.next_level as vip_next_level,f.operator as vip_operator,a.dn " +
		  "from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
		  "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
		  " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
		  s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
		  "group by uid,dn,dt")

//		memberWideTable.coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")


		//sparkSession.sql("select * from dws.dws_member limit 10")
		println("===================memberWideTable=========================")


		//查询当天增量信息
		//  TODO 当天增量表
		//  cast 对数据类型进行转换，cast('1' AS INT),将字符1转换成整数类型1
		//  decimal 对数据进行四舍五入操作，默认精度为0
		//  from_unixtime(bigint unixtime,string format) 将时间戳转换为指定格式日期，常与unix_timestamp一起使用
		//  unix_timestamp(string date,string pattern) 将指定时间格式的时间字符串转换为unix时间戳
		//  需求：针对dws层宽表的支付金额（paymoney）和vip等级(vip_level)这两个会变动的字段生成一张拉链表，需要一天进行一次更新
		val dayResult: Dataset[MemberZipper] = sparkSession.sql("select a.uid," +
		  "sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
		  s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time," +
		  "'9999-12-31' as end_time,first(a.dn) as dn " +
		  " from dwd.dwd_pcentermempaymoney a join " +
		  s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid").as[MemberZipper]

		//查询历史拉链表数据
		val historyResult: Dataset[MemberZipper] = sparkSession.sql("select *from dws.dws_member_zipper").as[MemberZipper]

		//两份数据根据用户id进行聚合 对end_time进行重新修改
		val memberZipperTable: Dataset[MemberZipperResult] = dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
		  //mapGroup在每个分组中进行map操作
		  .mapGroups {
			  case (key, iters) =>
				  val keys: Array[String] = key.split("_")
				  val uid: String = keys(0)
				  val dn: String = keys(1)
				  //对开始时间进行排序
				  val list: List[MemberZipper] = iters.toList.sortBy(item => item.start_time)

				  if (list.size > 1 && "9999-12-31".equals(list(list.size - 2).end_time)) {
					  //如果存在历史数据 需要对历史数据的end_time进行修改
					  //获取历史数据的最后一条数据
					  val oldLastModel = list(list.size - 2)
					  //获取当前时间最后一条数据
					  val lastModel = list(list.size - 1)
					  oldLastModel.end_time = lastModel.start_time
					  lastModel.paymoney = (BigDecimal.apply(lastModel.paymoney) + BigDecimal(oldLastModel.paymoney)).toString()
				  }
				  MemberZipperResult(list)
		  }

//		memberZipperTable.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper") //重组对象打散 刷新拉链表

		//sparkSession.sql("select * from dws.dws_member_zipper limit 10")
		println("===================memberZipperTable=========================")

	}
//	==============================================做题模块=======================================================//

	//维度表，一般是指对应一些业务状态，编号的解释表。也可以称之为码表。
	//比如地区表，订单状态，支付方式，审批状态，商品分类等等

	//基于dwd层基础表数据，需要对表进行维度退化进行表聚合，
	// 聚合成dws.dws_qz_chapter(章节维度表)，
	// dws.dws_qz_course（课程维度表），
	// dws.dws_qz_major(主修维度表)，
	// dws.dws_qz_paper(试卷维度表)，
	// dws.dws_qz_question(题目维度表)

	/**
	 * dws.dws_qz_chapter(章节维度表)
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsQzChapter(sparkSession: SparkSession, dt: String) = {

//		val result: DataFrame = sparkSession.sql("select a.chapterid,  a.chapterlistid,  a.chaptername,  a.sequence," +
//		  " a.showstatus,  a.status,  a.creator    as chapter_creator,  a.createtime as chapter_createtime," +
//		  " a.courseid   as chapter_courseid,  a.chapternum,  b.chapterallnum,  a.outchapterid,  b.chapterlistname, " +
//		  " c.pointid,  d.questionid,  d.questype,  c.pointname,  c.pointyear,  c.chapter,  c.excisenum,  c.pointlistid," +
//		  " c.pointdescribe,  c.pointlevel,  c.typelist,  c.score as point_score,  c.thought,  c.remid, " +
//		  " c.pointnamelist,  c.typelistids,  c.pointlist,  a.dt,  a.dn from dwd.dwd_qz_chapter a " +
//		  " inner join dwd.dwd_qz_chapter_list b  on a.chapterlistid = b.chapterlistid and a.dn = b.dn " +
//		  " inner join dwd.dwd_qz_point c  on a.chapterid = c.chapterid and a.dn = c.dn" +
//		  " inner join dwd.dwd_qz_point_question d  on c.pointid = d.pointid and c.dn = d.dn " +
//		  s"where a.dt = ${dt}")

		val dwdQzChapter = QzChapterDao.getDwdQzChapter(sparkSession, dt)
		val dwdQzChapterlist = QzChapterDao.getDwdQzChapterList(sparkSession, dt)
		val dwdQzPoint = QzChapterDao.getDwdQzPoint(sparkSession, dt)
		val dwdQzPointQuestion = QzChapterDao.getDwdQzPointQuestion(sparkSession, dt)
		val result = dwdQzChapter.join(dwdQzChapterlist, Seq("chapterlistid", "dn"))
		  .join(dwdQzPoint, Seq("chapterid", "dn"))
		  .join(dwdQzPointQuestion, Seq("pointid", "dn"))

		result.select("chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status",
			"chapter_creator", "chapter_createtime", "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname",
			"pointid", "questionid", "questype", "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe",
			"pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist", "dt", "dn")

		result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_chapter")

		//sparkSession.sql("select * from dws.dws_qz_chapter limit 10")
		println("===================saveDwsQzChapter=========================")

	}

	/**
	 * dws.dws_qz_course（课程维度表）
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsQzCourse(sparkSession: SparkSession, dt: String) = {

		val result: DataFrame = sparkSession.sql("select a.sitecourseid, a.siteid, a.courseid, " +
		  " a.sitecoursename, a.coursechapter, a.sequence, a.status, b.creator as sitecourse_creator, " +
		  " b.createtime as sitecourse_createtime, a.helppaperstatus, a.servertype, a.boardid, a.showstatus, " +
		  " b.majorid, b.coursename, b.isadvc, b.chapterlistid, b.pointlistid, c.courseeduid, c.edusubjectid," +
		  " a.dt, a.dn from dwd.dwd_qz_site_course a " +
		  " inner join dwd.dwd_qz_course b on a.courseid = b.courseid and a.dn = b.dn " +
		  " inner join dwd.dwd_qz_course_edusubject c on b.courseid = c.courseid and b.dn = c.dn  " +
		  s"where a.dt = ${dt}")

		result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_course")

		//sparkSession.sql("select * from dws.dws_qz_course limit 10")
		println("===================saveDwsQzCourse=========================")

	}

	/**
	 * dws.dws_qz_major(主修维度表)
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsQzMajor(sparkSession: SparkSession, dt: String) = {
		//读取主修表数据
		val dwdQzMajor = QzMajorDao.getQzMajor(sparkSession, dt)
		//读取做题网站数据
		val dwdQzWebsite = QzMajorDao.getQzWebsite(sparkSession, dt)
		//读取行业信息表数据
		val dwdQzBusiness = QzMajorDao.getQzBusiness(sparkSession, dt)
		//对三个表进行join数据整合
		val result = dwdQzMajor.join(dwdQzWebsite, Seq("siteid", "dn"))
		  .join(dwdQzBusiness, Seq("businessid", "dn"))
		  .select("majorid", "businessid", "siteid", "majorname", "shortname", "status", "sequence",
			  "major_creator", "major_createtime", "businessname", "sitename", "domain", "multicastserver", "templateserver",
			  "multicastgateway", "multicastport", "dt", "dn")
		result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_major")

		//sparkSession.sql("select * from dws.dws_qz_major limit 10")
		println("===================saveDwsQzMajor=========================")
	}



	/**
	 * dws.dws_qz_paper(试卷维度表)
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsQzPaper(sparkSession: SparkSession, dt: String) = {
		val dwdQzPaperView = QzPaperDao.getDwdQzPaperView(sparkSession, dt)
		val dwdQzCenterPaper = QzPaperDao.getDwdQzCenterPaper(sparkSession, dt)
		val dwdQzCenter = QzPaperDao.getDwdQzCenter(sparkSession, dt)
		val dwdQzPaper = QzPaperDao.getDwdQzPaper(sparkSession, dt)
		val result = dwdQzPaperView.join(dwdQzCenterPaper, Seq("paperviewid", "dn"), "left")
		  .join(dwdQzCenter, Seq("centerid", "dn"), "left")
		  .join(dwdQzPaper, Seq("paperid", "dn"))
		  .select("paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest"
			  , "contesttime", "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator",
			  " paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport",
			  " paperuseshow", "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
			  " stage", "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
			  " dt", "dn")

		result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_paper")

		//sparkSession.sql("select * from dws.dws_qz_paper limit 10")
		println("===================saveDwsQzPaper=========================")
	}

	/**
	 * dws.dws_qz_question(题目维度表)
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsQzQuestionTpe(sparkSession: SparkSession, dt: String) = {
		val dwdQzQuestion = QzQuestionDao.getQzQuestion(sparkSession, dt)
		val dwdQzQuestionType = QzQuestionDao.getQzQuestionType(sparkSession, dt)
		val result = dwdQzQuestion.join(dwdQzQuestionType, Seq("questypeid", "dn"))
		  .select("questionid", "parentid", "questypeid", "quesviewtype", "content", "answer", "analysis"
			  , " limitminute", "score", "splitscore", "status", "optnum", "lecture", "creator", "createtime", "modifystatus"
			  , " attanswer", "questag", "vanalysisaddr", "difficulty", "quesskill", "vdeoaddr", "viewtypename", "papertypename",
			  " remark", "splitscoretype", "dt", "dn")
		result.coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_qz_question")


		//sparkSession.sql("select * from dws.dws_qz_question limit 10")
		println("===================saveDwsQzQuestionTpe=========================")
	}


	/**
	 *  合成做题宽表
	 *
	 * @param sparkSession
	 * @param dt
	 */
	def saveDwsUserPaperDetail(sparkSession: SparkSession, dt: String) = {

		//withColumnRenamed :从命名列
		val dwdQzMemberPaperQuestion: DataFrame = UserPaperDetailDao.getDwdQzMemberPaperQuestion(sparkSession, dt)
		  .drop("paperid")
		  .withColumnRenamed("question_answer", "user_question_answer")

		val dwsQzChapter: DataFrame = UserPaperDetailDao.getDwsQzChapter(sparkSession, dt).drop("courseid")

		val dwsQzCourse: DataFrame = UserPaperDetailDao.getDwsQzCourse(sparkSession, dt)
		  .withColumnRenamed("sitecourse_creator", "course_creator")
		  .withColumnRenamed("sitecourse_createtime", "course_createtime")
		  .drop("majorid")
		  .drop("chapterlistid")
		  .drop("pointlistid")

		val dwsQzMajor = UserPaperDetailDao.getDwsQzMajor(sparkSession, dt)
		val dwsQzPaper = UserPaperDetailDao.getDwsQzPaper(sparkSession, dt).drop("courseid")
		val dwsQzQuestion = UserPaperDetailDao.getDwsQzQuestion(sparkSession, dt)

		val wideTable: DataFrame = dwdQzMemberPaperQuestion.join(dwsQzCourse, Seq("sitecourseid", "dn")).
		  join(dwsQzChapter, Seq("chapterid", "dn")).join(dwsQzMajor, Seq("majorid", "dn"))
		  .join(dwsQzPaper, Seq("paperviewid", "dn")).join(dwsQzQuestion, Seq("questionid", "dn"))
		  .select("userid", "courseid", "questionid", "useranswer", "istrue", "lasttime", "opertype",
			  "paperid", "spendtime", "chapterid", "chaptername", "chapternum",
			  "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questype", "pointyear", "chapter", "pointname"
			  , "excisenum", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist",
			  "typelistids", "pointlist", "sitecourseid", "siteid", "sitecoursename", "coursechapter", "course_sequence", "course_status"
			  , "course_creator", "course_createtime", "servertype", "helppaperstatus", "boardid", "showstatus", "majorid", "coursename",
			  "isadvc", "chapterlistid", "pointlistid", "courseeduid", "edusubjectid", "businessid", "majorname", "shortname",
			  "major_status", "major_sequence", "major_creator", "major_createtime", "businessname", "sitename",
			  "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport", "paperviewid", "paperviewname", "paperparam",
			  "openstatus", "explainurl", "iscontest", "contesttime", "conteststarttime", "contestendtime", "contesttimelimit",
			  "dayiid", "paper_status", "paper_view_creator", "paper_view_createtime", "paperviewcatid", "modifystatus", "description", "paperuse",
			  "testreport", "centerid", "paper_sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype",
			  "paper_stage", "papercatid", "paperyear", "suitnum", "papername", "totalscore", "question_parentid", "questypeid",
			  "quesviewtype", "question_content", "question_answer", "question_analysis", "question_limitminute", "score",
			  "splitscore", "lecture", "question_creator", "question_createtime", "question_modifystatus", "question_attanswer",
			  "question_questag", "question_vanalysisaddr", "question_difficulty", "quesskill", "vdeoaddr", "question_description",
			  "question_splitscoretype", "user_question_answer", "dt", "dn")

		wideTable.show()
		while(true){
			println("ok")
		}

//		  .coalesce(1).write.mode(SaveMode.Append).insertInto("dws.dws_user_paper_detail")

		//sparkSession.sql("select * from dws.dws_user_paper_detail limit 10")


//		println("===================saveDwsUserPaperDetail=========================")
	}


	def test(sparkSession: SparkSession, dt: String) = {

		import sparkSession.implicits._ //隐式转换

		val dwdMember: Dataset[Row] = DwdMemberDao.getDwdMember(sparkSession).where(s"dt='${dt}'") //主表用户表
		val dwdMemberRegtype: DataFrame = DwdMemberDao.getDwdMemberRegType(sparkSession)
		val dwdBaseAd: DataFrame = DwdMemberDao.getDwdBaseAd(sparkSession)
		val dwdBaseWebsite: DataFrame = DwdMemberDao.getDwdBaseWebSite(sparkSession)
		val dwdPcentermemPaymoney: DataFrame = DwdMemberDao.getDwdPcentermemPayMoney(sparkSession)
		val dwdVipLevel: DataFrame = DwdMemberDao.getDwdVipLevel(sparkSession)

		//TODO 默认是sortMerge Join,12个stage中有4个stage是join
		// 优化小表,使用broadcast Join 适合大表join小表,广播小表,数据在Driver端,广播分发数据需要时间,分发时间>数据处理时间,不建议使用

		//1 广播变量:时间明显加快
		import org.apache.spark.sql.functions.broadcast

		val dataset: Dataset[DwsMember] = dwdMember.join(dwdMemberRegtype, Seq("uid", "dn"), "left_outer")
		  //		  		  .join(dwdBaseAd , Seq("ad_id", "dn"), "left_outer")
		  .join(broadcast(dwdBaseAd), Seq("ad_id", "dn"), "left_outer")
		  //		  .join(dwdBaseWebsite  , Seq("siteid", "dn"), "left_outer")
		  .join(broadcast(dwdBaseWebsite), Seq("siteid", "dn"), "left_outer")
		  //		  .join(dwdPcentermemPaymoney, Seq("uid", "dn"), "left_outer")
		  .join(broadcast(dwdPcentermemPaymoney), Seq("uid", "dn"), "left_outer")
		  //		  .join(dwdVipLevel, Seq("vip_id", "dn"), "left_outer")
		  .join(broadcast(dwdVipLevel), Seq("vip_id", "dn"), "left_outer")
		  .select("uid", "ad_id", "fullname", "iconurl", "lastlogin", "mailaddr", "memberlevel", "password"
			  , "paymoney", "phone", "qq", "register", "regupdatetime", "unitname", "userip", "zipcode", "appkey"
			  , "appregurl", "bdp_uuid", "reg_createtime", "domain", "isranreg", "regsource", "regsourcename", "adname"
			  , "siteid", "sitename", "siteurl", "site_delete", "site_createtime", "site_creator", "vip_id", "vip_level",
			  "vip_start_time", "vip_end_time", "vip_last_modify_time", "vip_max_free", "vip_min_free", "vip_next_level"
			  , "vip_operator", "dt", "dn").as[DwsMember]
		//Dataset默认缓存级别为memory_and_disk,内存不够就会溢写磁盘
//		dataset.cache()//999.8MB
		//RDD默认缓存级别为memory_only
//		dataset.rdd.cache() //3.1GB

		//TODO 缓存优化:kryo序列化,默认是Java序列化;
		// sparkConf中设置序列化模式
		// 相应的样例类注册到kryo序列化中,更改指定样例类缓存级别
		// DF和 DS 性能极好,使用堆外内存,内部已经直线kryo序列化,rdd需要手动实现,但是rdd实现序列化,对CPU极其不友好
		dataset.rdd.persist(StorageLevel.MEMORY_AND_DISK_SER) //4.9G 落盘16.1G

		//只有执行action算子才会在spark WebUI中的storage中显示
		dataset.foreach(item=> println(item.adname))

		//TODO 优化并行度;并行度默认200



//		val wideTable: Dataset[DwsMember_Result] = dataset.groupByKey((item: DwsMember) => item.uid + "_" + item.dn)
//		  .mapGroups { case (key, iters) =>
//			  val keys = key.split("_")
//			  val uid = Integer.parseInt(keys(0))
//			  val dn = keys(1)
//			  val dwsMembers = iters.toList
//			  val paymoney = dwsMembers.filter(_.paymoney != null).map(_.paymoney).reduceOption(_ + _).getOrElse(BigDecimal.apply(0.00)).toString
//			  val ad_id = dwsMembers.map(_.ad_id).head
//			  val fullname = dwsMembers.map(_.fullname).head
//			  val icounurl = dwsMembers.map(_.iconurl).head
//			  val lastlogin = dwsMembers.map(_.lastlogin).head
//			  val mailaddr = dwsMembers.map(_.mailaddr).head
//			  val memberlevel = dwsMembers.map(_.memberlevel).head
//			  val password = dwsMembers.map(_.password).head
//			  val phone = dwsMembers.map(_.phone).head
//			  val qq = dwsMembers.map(_.qq).head
//			  val register = dwsMembers.map(_.register).head
//			  val regupdatetime = dwsMembers.map(_.regupdatetime).head
//			  val unitname = dwsMembers.map(_.unitname).head
//			  val userip = dwsMembers.map(_.userip).head
//			  val zipcode = dwsMembers.map(_.zipcode).head
//			  val appkey = dwsMembers.map(_.appkey).head
//			  val appregurl = dwsMembers.map(_.appregurl).head
//			  val bdp_uuid = dwsMembers.map(_.bdp_uuid).head
//			  val reg_createtime = dwsMembers.map(_.reg_createtime).head
//			  val domain = dwsMembers.map(_.domain).head
//			  val isranreg = dwsMembers.map(_.isranreg).head
//			  val regsource = dwsMembers.map(_.regsource).head
//			  val regsourcename = dwsMembers.map(_.regsourcename).head
//			  val adname = dwsMembers.map(_.adname).head
//			  val siteid = dwsMembers.map(_.siteid).head
//			  val sitename = dwsMembers.map(_.sitename).head
//			  val siteurl = dwsMembers.map(_.siteurl).head
//			  val site_delete = dwsMembers.map(_.site_delete).head
//			  val site_createtime = dwsMembers.map(_.site_createtime).head
//			  val site_creator = dwsMembers.map(_.site_creator).head
//			  val vip_id = dwsMembers.map(_.vip_id).head
//			  val vip_level = dwsMembers.map(_.vip_level).max
//			  val vip_start_time = dwsMembers.map(_.vip_start_time).min
//			  val vip_end_time = dwsMembers.map(_.vip_end_time).max
//			  val vip_last_modify_time = dwsMembers.map(_.vip_last_modify_time).max
//			  val vip_max_free = dwsMembers.map(_.vip_max_free).head
//			  val vip_min_free = dwsMembers.map(_.vip_min_free).head
//			  val vip_next_level = dwsMembers.map(_.vip_next_level).head
//			  val vip_operator = dwsMembers.map(_.vip_operator).head
//
//			  DwsMember_Result(uid, ad_id, fullname, icounurl, lastlogin, mailaddr, memberlevel, password, paymoney,
//				  phone, qq, register, regupdatetime, unitname, userip, zipcode, appkey, appregurl,
//				  bdp_uuid, reg_createtime, domain, isranreg, regsource, regsourcename, adname, siteid,
//				  sitename, siteurl, site_delete, site_createtime, site_creator, vip_id, vip_level,
//				  vip_start_time, vip_end_time, vip_last_modify_time, vip_max_free, vip_min_free,
//				  vip_next_level, vip_operator, dt, dn)
//		  }

//		wideTable.show()
		while(true){
			println("ok! Go")
		}

	}

}
