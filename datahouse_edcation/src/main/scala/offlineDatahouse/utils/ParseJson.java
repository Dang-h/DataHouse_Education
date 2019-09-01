package offlineDatahouse.utils;

import com.alibaba.fastjson.JSONObject;


public class ParseJson {
	/**
	 * 将string解析成JSON对象
	 * @param data
	 * @return 是,返回JSON;否,返回null
	 */

	public static JSONObject getJsonData(String data) {
		try {
			return JSONObject.parseObject(data);
		} catch (Exception e) {
			return null;
		}
	}
}
