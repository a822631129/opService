package iie.ihadoop.client;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;

public class GetClient {

	public static void main(String[] args) throws Exception {
		GetMethod getmethod = new GetMethod(args[0]);
		HttpClient httpclient = new HttpClient();
		httpclient.executeMethod(getmethod);
		String s = getmethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		System.out.println(s);
	}

}
