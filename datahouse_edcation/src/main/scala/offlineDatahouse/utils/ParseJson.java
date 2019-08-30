package offlineDatahouse.utils;

import com.alibaba.fastjson.JSONObject;

public class ParseJson {
	/**
	 * 将string解析成JSON对象
	 * @param data
	 * @return
	 */
	public static JSONObject getJsonData(String data) {
		try {
			return JSONObject.parseObject(data);
		} catch (Exception e) {
			return null;
		}
	}
}
