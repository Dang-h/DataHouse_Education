package offlineDatahouse.controller

import offlineDatahouse.bean.MemberLog
import offlineDatahouse.utils.HiveUtil
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ETL {
	def main(args: Array[String]): Unit = {
		val ss: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName("ETL").getOrCreate()
		val dataFrame: DataFrame = ss.read.json("hdfs://hadoop102:9000/user/atguigu/ods/member.log")

		import ss.implicits._
		val memberInfoDataSet: Dataset[MemberLog] = dataFrame.as[MemberLog]
		val etlDateSet: Dataset[MemberLog] = memberInfoDataSet.map(lines => {
			lines.fullname = lines.fullname.charAt(0) + "XX"
			lines.phone = lines.phone.splitAt(3)._1 + "*****" + lines.phone.splitAt(8)._2
			lines.password = "******"
			lines
		})

		HiveUtil.openDynamicPartition(ss) //开启动态分区
		HiveUtil.openCompression(ss) //开启压缩
		HiveUtil.useSnappyCompression(ss) //使用snappy压缩

		etlDateSet.write.mode(SaveMode.Append).insertInto("dwd.dwd_member")

		ss.stop()
	}

}
