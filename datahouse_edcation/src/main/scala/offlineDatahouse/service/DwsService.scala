package offlineDatahouse.service

import offlineDatahouse.bean.{MemberZipper, MemberZipperResult}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DwsService {

	/**
	 * 将6张基础表合并成一张宽表,并根据宽表支付金额和vip等级生成拉链表
	 *
	 * @param sparkSession
	 * @param time
	 */
	def importMember(sparkSession: SparkSession, time:String)={
		import sparkSession.implicits._
		//查询全量数据 刷新到 TODO 宽表
		//宽表，以用户信息(dwd_member)为主表，在主表基础上并入其他明细表字段
		//以uid（用户id）和dn（网站分区）为条件用主表left join 其他表，再根据日期来过滤得到数据
		//s的作用：在字符串上下文的相应部分之间插入参数。
		 sparkSession.sql("select uid,first(ad_id),first(fullname),first(iconurl),first(lastlogin)," +
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
		  s"from dwd.dwd_member a left join dwd.dwd_member_regtype b on a.uid=b.uid " +
		  "and a.dn=b.dn left join dwd.dwd_base_ad c on a.ad_id=c.adid and a.dn=c.dn left join " +
		  " dwd.dwd_base_website d on b.websiteid=d.siteid and b.dn=d.dn left join dwd.dwd_pcentermempaymoney e" +
		  s" on a.uid=e.uid and a.dn=e.dn left join dwd.dwd_vip_level f on e.vip_id=f.vip_id and e.dn=f.dn where a.dt='${time}')r  " +
		  "group by uid,dn,dt").coalesce(1).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member")


//			sparkSession.sql("select * from dws.dws_member limit 10").show(false)

		//查询当天增量信息
		//  TODO 当天增量表
		//  cast 对数据类型进行转换，cast('1' AS INT),将字符1转换成整数类型1
		//  decimal 对数据进行四舍五入操作，默认精度为0
		//  from_unixtime(bigint unixtime,string format) 将时间戳转换为指定格式日期，常与unix_timestamp一起使用
		//  unix_timestamp(string date,string pattern) 将指定时间格式的时间字符串转换为unix时间戳
		//  需求：针对dws层宽表的支付金额（paymoney）和vip等级(vip_level)这两个会变动的字段生成一张拉链表，需要一天进行一次更新
		val dayResult: Dataset[MemberZipper] = sparkSession.sql(s"select a.uid,sum(cast(a.paymoney as decimal(10,4))) as paymoney,max(b.vip_level) as vip_level," +
		  s"from_unixtime(unix_timestamp('$time','yyyyMMdd'),'yyyy-MM-dd') as start_time,'9999-12-31' as end_time,first(a.dn) as dn " +
		  " from dwd.dwd_pcentermempaymoney a join " +
		  s"dwd.dwd_vip_level b on a.vip_id=b.vip_id and a.dn=b.dn where a.dt='$time' group by uid").as[MemberZipper]

		//查询历史拉链表数据
		val historyResult: Dataset[MemberZipper] = sparkSession.sql("select *from dws.dws_member_zipper").as[MemberZipper]

//		dayResult.show(false)

		//两份数据根据用户id进行聚合 对end_time进行重新修改
		val reuslt = dayResult.union(historyResult).groupByKey(item => item.uid + "_" + item.dn)
		  //mapGroup在每个分组中进行map操作
		  .mapGroups { case (key, iters) =>
			  val keys: Array[String] = key.split("_")
			  val uid: String = keys(0)
			  val dn = keys(1)
			  val list: List[MemberZipper] = iters.toList.sortBy(item => item.start_time) //对开始时间进行排序
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
		  }.flatMap(_.list).coalesce(3).write.mode(SaveMode.Overwrite).insertInto("dws.dws_member_zipper") //重组对象打散 刷新拉链表

	}



}
