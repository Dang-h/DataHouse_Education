package offlineDatahouse.utils

import org.apache.spark.sql.SparkSession

object HiveUtil {
	/**
	 * 开启snappy压缩
	 * 当mapreduce作业的map输出的数据比较大的时候，
	 * 作为map到reduce的中间数据的压缩格式；或者作为一个mapreduce作业的输出和另外一个mapreduce作业的输入
	 *
	 * @param sparkSession
	 * @return
	 */
	def useSnappyCompression(sparkSession: SparkSession) = {
		sparkSession.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
		sparkSession.sql("set mapreduce.output.fileoutputformat.compress=true")
		sparkSession.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
	}


	/**
	 * 开启压缩
	 *
	 * @param sparkSession
	 * @return
	 */
	def openCompression(sparkSession: SparkSession) = {
		//最终输出结果压缩
		sparkSession.sql("set mapred.output.compress=true")
		sparkSession.sql("set hive.exec.compress.output=true")
	}


	/**
	 * 开启动态分区：
	 * 对分区表 Insert 数据时候，数据库自动会根据分区字段的值，将数据插入到相应的分区中
	 *
	 * @param sparkSession
	 * @return
	 */
	def openDynamicPartition(sparkSession: SparkSession) = {
		sparkSession.sql("set hive.exec.dynamic.partition=true")
		//设置为非严格模式：允许所有的分区字段都可以使用动态分区
		sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
	}


}
