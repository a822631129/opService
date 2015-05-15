package iie.ihadoop.service;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import iie.ihadoop.db.HiveMDAO;
import iie.ihadoop.db.UdfDAO;
import iie.ihadoop.model.Udf;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;

/**
 * UDF管理模块
 * 
 * @author sunyang
 *
 */
@Path("/udfs")
public class Udfs {

	Udf udf = new Udf();
	UdfDAO udfDao = new UdfDAO();

	/**
	 * 注册udf
	 * @param request 请求
	 * @return
	 */
	@POST
	public String registerUdf(@Context HttpServletRequest request) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		InputStream in = request.getInputStream();
		String requestFilePath = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) 
				+ "/" + "registerUdf/" + serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> req = parseRegisterRequest(requestFilePath); // 解析xml请求参数
		String udfname = udf.getUdfname().toLowerCase();
		udf.setRegistertime(serialName);
		if (!"".equals(req.get("error"))) { //请求体内容有错误
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", req.get("error"));
			return JavaUtil.createXml1("error", map);
		}

		if (existSystemfunc(udfname)) { //检验是否是系统函数
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is system function, please use a different name.");
			return JavaUtil.createXml1("error", map);
		}

		if (existUdfunc(udfname)) { //检验是否已注册
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is exist, please use a different name.");
			return JavaUtil.createXml1("error", map);
		}
		
		HiveMDAO hmDao = new HiveMDAO();
		if (hmDao.hasFunc(udfname)){ //检验hive元数据是否注册
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is exist in HiveMetaStore, please use a different name.");
			return JavaUtil.createXml1("error", map);
		}
		if(!"ok".equals(checkUdf())) { //检验请求参数值长度
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", checkUdf());
			return JavaUtil.createXml1("error", map);
		}

		//执行注册命令
		String[] cmd = {"/usr/bin/hive", "-e", "\"create function " + udfname + " as '" + udf.getClassname()
						+ "' using jar '" + InitConfig.FUNCTIONJARPATH + udf.getjarname() + "'\"" };
		Process p = Runtime.getRuntime().exec(cmd);

		System.out.println(">>>>>>>>>>>>>>>>>>getInputStream>>>>>>>>>>>>>>>>");
		System.out.println(JavaUtil.convertStreamToString(p.getInputStream()));
		System.out.println(">>>>>>>>>>>>>>>>>>getErrorStream>>>>>>>>>>>>>>>>");
		System.out.println(JavaUtil.convertStreamToString(p.getErrorStream()));
		p.waitFor();
		
        
		if (hmDao.hasFunc(udfname)){ //检验hive元数据是否注册成功
		    udfDao.insertUdf(udf); //插入业务数据数据库udf记录
		} else {
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "hive元数据注册失败");
			return JavaUtil.createXml1("error", map); 
		}

		Map<String, String> map = new HashMap<String, String>();
		map.put("udfname", udfname);
		map.put("status", "success");
		return JavaUtil.createXml1("response", map);
	}

	/**
	 * 删除udf
	 * @param udfname udf名称
	 * @return
	 * @throws Exception
	 */
	@DELETE
	@Path("/{udfname}")
	public String deleteUdf(@PathParam("udfname") String udfname) throws Exception {
		udfname = udfname.toLowerCase();
		if (!existUdfunc(udfname)) { //检验是否注册
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is not exist.");
			return JavaUtil.createXml1("error", map);
		}
		//执行删除udf命令
		String[] cmd = new String[] { "/usr/bin/hive", "-e", "drop function if exists " + udfname };
		Process p = Runtime.getRuntime().exec(cmd);
		System.out.println(">>>>>>>>>>>>>>>>>>getInputStream>>>>>>>>>>>>>>>>");
		System.out.println(JavaUtil.convertStreamToString(p.getInputStream()));
		System.out.println(">>>>>>>>>>>>>>>>>>getErrorStream>>>>>>>>>>>>>>>>");
		System.out.println(JavaUtil.convertStreamToString(p.getErrorStream()));

		HiveMDAO hmDao = new HiveMDAO();
		if (!hmDao.hasFunc(udfname)){ //检验是否hive元数据是否删除成功
			udfDao.deleteUdf(udfname); //删除业务数据库记录
		} else {
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "hive元数据删除失败");
			return JavaUtil.createXml1("error", map); 
		}

		Map<String, String> map = new HashMap<String, String>();
		map.put("udfname", udfname);
		map.put("status", "success");
		return JavaUtil.createXml1("response", map);
	}

	/**
	 * 更新udf
	 * @param request 请求
	 * @param udfname udf名称
	 * @return
	 */
	@PUT
	@Path("/{udfname}")
	public String updateUdf(@Context HttpServletRequest request,
			@PathParam("udfname") String udfname) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		udfname = udfname.toLowerCase();
		udf.setUdfname(udfname);
		udf.setRegistertime(serialName.substring(0, 19));
		InputStream in = request.getInputStream();
		String requestFilePath1 = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "registerUdf/"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath1); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> req = parseUpdateRequest(requestFilePath1); // 解析xml请求参数
		if (!"".equals(req.get("error"))) { //请求体内容有错误
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", req.get("error"));
			return JavaUtil.createXml1("error", map);
		}

		if (!existUdfunc(udfname)) { //是否已注册
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is not exist");
			return JavaUtil.createXml1("error", map);
		} 
		udfDao.updateUdf(udf); //更新业务数据库记录

		Map<String, String> map = new HashMap<String, String>();
		map.put("udfname", udfname);
		map.put("status", "success");
		return JavaUtil.createXml1("response", map);
	}

	/**
	 * 查看所有注册的udf列表
	 * @return
	 * @throws Exception
	 */
	@GET
	public String getUdfs() throws Exception {
		List<String> udfnames = udfDao.getUdfnames();
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		for (int i = 0; i < udfnames.size(); i++) {
			Element udfnameElement = rootElement.addElement("udfname");
			udfnameElement.setText(udfnames.get(i));
		}
		return JavaUtil.formatXML(document);
	}

	/**
	 * 查看某个udf详情
	 * @param udfname udf名称
	 * @return
	 * @throws Exception
	 */
	@GET
	@Path("/{udfname}")
	public String getUdf(@PathParam("udfname") String udfname) throws Exception {
		if (!existUdfunc(udfname)) { //检验是否注册
			Map<String, String> map = new HashMap<String, String>();
			map.put("udfname", udfname);
			map.put("errmsg", "thd udfname '" + udfname + "' is not exist");
			return JavaUtil.createXml1("error", map);
		}
		Udf udf = udfDao.getUdfByUdfame(udfname);
		return createGetUdfResponse(udf); 
	}

	private String checkUdf() {
		if(udf.getUdfname().length() > 64) {
			return "udfname长度不能大于64";
		}
		if(udf.getClassname().length() > 64) {
			return "classname长度不能大于64";
		}
		if(udf.getjarname().length() > 64) {
			return "jarname长度不能大于64";
		}
		if(udf.getAuthor().length() > 64) {
			return "author长度不能大于64";
		}
		if(udf.getDescriptor().length() > 1024) {
			return "descriptor长度不能大于1024";
		}
		return "ok";
	}
	
	/**
	 * 创建查看udf详情的返回xml格式信息
	 * @param udf udf实体
	 * @return
	 * @throws Exception
	 */
	private String createGetUdfResponse(Udf udf) throws Exception {
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		Element udfdescriptor = rootElement.addElement("descriptor");
		Element udfnameElement = udfdescriptor.addElement("udfname");
		Element classnameElement = udfdescriptor.addElement("classname");
		Element jarnameElement = udfdescriptor.addElement("jarname");
		Element authorElement = udfdescriptor.addElement("author");
		Element registertimeElement = udfdescriptor.addElement("registertime");
		Element descriptorElement = udfdescriptor.addElement("descriptor");
		udfnameElement.setText(udf.getUdfname());
		classnameElement.setText(udf.getClassname());
		jarnameElement.setText(udf.getjarname());
		authorElement.setText(udf.getAuthor());
		registertimeElement.setText(udf.getRegistertime());
		descriptorElement.setText(udf.getDescriptor());
		return JavaUtil.formatXML(document);
	}

	/**
	 * 和hive系统函数比较，判断是否已存在
	 * 
	 * @param udfname udf名称
	 * @return
	 */
	private boolean existSystemfunc(String udfname) {
		for (int i = 0; i < Udf.SYS_FUNCTIONS.length; i++) {
			if (udfname.equals(Udf.SYS_FUNCTIONS[i])) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 和已注册的udf比较，判断名称是否已注册。
	 * 
	 * @param udfname udf名称
	 * @return
	 * @throws Exception
	 */
	private boolean existUdfunc(String udfname) throws Exception {
		List<String> udfnames = udfDao.getUdfnames();
		for (int j = 0; j < udfnames.size(); j++) {
			if (udfname.equals(udfnames.get(j))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 解析“udf注册”请求体内容，返回相应的验证信息
	 * 
	 * @param xmlFilePath 请求信息保存地址
	 * @return
	 * @throws Exception
	 */
	private Map<String, String> parseRegisterRequest(String xmlFilePath) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("error", "");
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		Element rootElement = document.getRootElement(); // 获得根节点
		if (rootElement.selectSingleNode("udfdescriptor/udfname") != null) {
			udf.setUdfname(rootElement.selectSingleNode("udfdescriptor/udfname").getText());	
		} else {
			udf.setUdfname("");
			map.put("error", "udfname is null");
		}
		if (rootElement.selectSingleNode("udfdescriptor/classname") != null) {
			udf.setClassname(rootElement.selectSingleNode("udfdescriptor/classname").getText());	
		} else {
			map.put("error", "classname is null");
		}
		if (rootElement.selectSingleNode("udfdescriptor/jarname") != null) {
			udf.setjarname(rootElement.selectSingleNode("udfdescriptor/jarname").getText());
		} else {
			map.put("error", "jarname is null");
		}
		if (rootElement.selectSingleNode("udfdescriptor/descriptor") != null) {
			udf.setDescriptor(rootElement.selectSingleNode("udfdescriptor/descriptor").getText());
		} else {
			map.put("error", "descriptor is null");
		}
		if (rootElement.selectSingleNode("udfdescriptor/author") != null) {
			udf.setAuthor(rootElement.selectSingleNode("udfdescriptor/author").getText());
		} else {
			map.put("error", "author is null");
		}	
		return map;
	}

	/**
	 * 解析“udf更新”请求体内容，返回相应验证信息。
	 * 
	 * @param xmlFilePath 请求信息保存地址
	 * @return
	 * @throws Exception
	 */
	private Map<String, String> parseUpdateRequest(String xmlFilePath)
			throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("error", "");
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		Element rootElement = document.getRootElement(); // 获得根节点
		if (rootElement.selectSingleNode("descriptor") != null) {
			if(rootElement.selectSingleNode("descriptor").getText().length() > 1024) {
				map.put("error", "descriptor长度不能大于1024");
				return map;
			}
			map.put("descriptor", rootElement.selectSingleNode("descriptor").getText());	
		} else {
			map.put("error", "descriptor is null");
			return map;
		}
		return map;
	}

	/**
	 * 测试方法
	 * 
	 * @param resultParseRequest
	 * @param udfname
	 * @throws Exception
	 */
	public void hiveClientRegisterUdf(Map<String, String> resultParseRequest,
			String udfname) throws Exception {
		HiveConf hiveconf = new HiveConf();
		HiveMetaStoreClient hivemsclient = new HiveMetaStoreClient(hiveconf);
		List<ResourceUri> listUri = new ArrayList<ResourceUri>();
		ResourceUri uri = new ResourceUri();
		uri.setUri(InitConfig.FUNCTIONJARPATH
				+ resultParseRequest.get("jarname"));
		uri.setResourceType(ResourceType.findByValue(1));
		listUri.add(uri);
		Function func = new Function(udfname, "default",
				resultParseRequest.get("classname"),
				resultParseRequest.get("author"), PrincipalType.findByValue(1),
				1, FunctionType.findByValue(1), listUri);
		hivemsclient.createFunction(func);
	}
}
