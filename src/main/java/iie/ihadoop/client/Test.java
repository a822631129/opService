package iie.ihadoop.client;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;

public class Test {

	public static void main(String[] args) throws Exception {
		getMethod("http://192.168.8.205:8085/cmu/getyn");
	}
	
	private static String getMethod(String url) throws Exception {
		GetMethod getmethod = new GetMethod(url);
		HttpClient httpclient = new HttpClient();
		System.out.println("发送请求》》》");
		httpclient.executeMethod(getmethod);
		String s = getmethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		System.out.println("response>>>>>>" + s);
		return s;
	}
}
