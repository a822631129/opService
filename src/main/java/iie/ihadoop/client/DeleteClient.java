package iie.ihadoop.client;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.DeleteMethod;

public class DeleteClient {

	public static void main(String[] args) throws Exception {
		DeleteMethod deletemethod = new DeleteMethod(args[0]);
		HttpClient client = new HttpClient();
		client.executeMethod(deletemethod);
		String s = deletemethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		System.out.println(s);
	}

}
