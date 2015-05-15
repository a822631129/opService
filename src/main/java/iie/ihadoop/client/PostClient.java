package iie.ihadoop.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.ByteArrayRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.RequestEntity;

public class PostClient {

	public static void main(String[] args) throws Exception {
		if (args.length == 2) {
			PostMethod filePost = new PostMethod(args[0]);// post请求
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
			filePost.setRequestEntity(requestEntity);
			// 执行请求
			HttpClient client = new HttpClient();
			client.executeMethod(filePost);
			// 相应信息
			String s = filePost.getResponseBodyAsString();
			s = new String(s.getBytes("ISO8859-1"), "UTF-8");
			System.out.println(s);
		} else if (args.length == 4) {
			System.out.println("开始导入算子包。。。");
			System.out.println("从节点" + args[2] + "中的路径" + args[3]
					+ "把算子导入到本机的/usr/local/ops/" + args[1] + "下。。。");

			PostMethod opPost = new PostMethod(args[0]);// post请求

			String Params = "";
			Params = "opName=" + args[1] + "&" + "opIp=" + args[2] + "&"
					+ "opPath=" + args[3];

			RequestEntity requestEntity = new ByteArrayRequestEntity(
					Params.getBytes());
			opPost.setRequestEntity(requestEntity);
			// 执行请求
			HttpClient client = new HttpClient();
			client.executeMethod(opPost);
			// 相应信息
			String s = opPost.getResponseBodyAsString();
			s = new String(s.getBytes("ISO8859-1"), "UTF-8");
			System.out.println(s);
		}

	}
}
