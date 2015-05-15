package iie.ihadoop.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;

public class PutClient {

	public static void main(String[] args) throws Exception {
		PutMethod putmethod = new PutMethod(args[0]);
		File file = new File(args[1]);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		String xmlParams = "";
		String tempString = null;
		// 一次读入一行，直到读入null为文件结束
		while ((tempString = reader.readLine()) != null) {
			xmlParams += "\n" + tempString;
		}
		reader.close();
		xmlParams = xmlParams.substring(1);
		RequestEntity requestEntity = new ByteArrayRequestEntity(
				xmlParams.getBytes());
		putmethod.setRequestEntity(requestEntity);
		// 执行请求
		HttpClient client = new HttpClient();
		client.executeMethod(putmethod);
		// 相应信息
		String s = putmethod.getResponseBodyAsString();
		s = new String(s.getBytes("ISO8859-1"), "UTF-8");
		System.out.println(s);

	}

}
