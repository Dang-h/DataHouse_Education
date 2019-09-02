package offlineDatahouse.bean.trans

/**
 * @ClassName QzQuestion
 * @Description TODO
 * @Author Dang-h
 * @Email 54danghao@gmail.com
 * @Date 2019-9-2 0002 9:54
 * @Version 1.0
 *
 **/
case class QzQuestion(questionid: Int,
					  parentid: Int,
					  questypeid: Int,
					  quesviewtype: Int,
					  content: String,
					  answer: String,
					  analysis: String,
					  limitminute: String,
					  scoe: BigDecimal,
					  splitcore: BigDecimal,
					  status: String,
					  optnum: Int,
					  lecture: String,
					  creator: String,
					  createtime: String,
					  modifystatus: String,
					  attanswer: String,
					  questag: String,
					  vanalysisaddr: String,
					  difficulty: String,
					  quesskill: String,
					  vdeoaddr: String,
					  dt: String,
					  dn: String)
