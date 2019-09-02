package offlineDatahouse.bean.userInfo

case class MemberZipper(uid: Int,
						var paymoney: String,
						vip_level: String,
						start_time: String,
						var end_time: String,
						dn: String)

case class MemberZipperResult(list:List[MemberZipper])
