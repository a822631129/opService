package iie.ihadoop.service;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import iie.ihadoop.db.OpTaskDAO;
import iie.ihadoop.util.HdfsUtil;
import iie.ihadoop.util.HiveUtil;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;

/**
 * 存储资源管理模块
 * 
 * @author sunyang
 *
 */
@Path("/storagespace")
public class StorageSpace {
	
	OpTaskDAO opTaskDao = new OpTaskDAO();
	HdfsUtil hdfsUtil = new HdfsUtil();
	HiveUtil hiveUtil = new HiveUtil();

	/**
	 * 创建存储单元(创建目录、设定配额、创建数据库)
	 * @POST 响应POST请求
	 * @Path 定义路径
	 * @param request 请求
	 * @param groupName 存储单元名
	 * @return 返回创建信息
	 * @throws Exception
	 *             抛出异常
	 */
	@POST
	@Path("/{groupName}")
	public String createStorageSpace(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		InputStream in = request.getInputStream();
		String requestFilePath1 = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "createStorageSpace/" 
				+ groupName + "_" + serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath1); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> resultParseRequest = parseRequest(requestFilePath1); // 解析xml请求参数
		if (!"".equals(resultParseRequest.get("error"))) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(resultParseRequest.get("error"))); //返回错误信息
		}
		String groupPath = InitConfig.ROOT_PATH + groupName;
		if (hdfsUtil.existFile(groupPath)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the dir '" + groupName + "' is exist")); 
		}
		if (hiveUtil.existDB("tmp_" + groupName) || hiveUtil.existDB("result_" + groupName)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the database 'result_" + groupName + "' or 'tmp_" + groupName + "' is exist"));
		}

		createDir(groupName); // 创建目录 
		setSpaceQuota(groupName, resultParseRequest.get("quota")); // 设定配额
		createDB(groupName); // 创建数据库
		
		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("tmpdbname", "tmp_" + groupName);
		mapsuc.put("tmpfilepath", InitConfig.ROOT_PATH + groupName + "/tmp_dir");
		mapsuc.put("resultdbname", "result_" + groupName);
		mapsuc.put("resultfilepath", InitConfig.ROOT_PATH + groupName + "/result_dir");
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc); //生成xml响应
	}

	/**
	 * 查看存储单元配额
	 * 
	 * @GET 响应GET请求
	 * @Path 定义路径
	 * @param groupName 存储单元名
	 * @return 返回配额
	 * @throws Exception 抛出异常
	 */
	@GET
	@Path("/{groupName}")
	public String getSpaceQuota(@PathParam("groupName") String groupName) throws Exception {
		String groupPath = InitConfig.ROOT_PATH + groupName;
		if (hdfsUtil.existFile(groupPath)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the path '" + groupPath.toString() + "' is not exist")); //返回错误信息
		}
		String theQuota = hdfsUtil.getSpaceQuota(groupPath); //获得目录配额
		
		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("quota", theQuota);
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}

	/**
	 * 修改配额
	 * 
	 * @PUT 响应PUT请求
	 * @Path 定义路径
	 * @param request 请求
	 * @param groupName 存储单元名
	 * @return 返回配额
	 * @throws Exception 抛出异常
	 */
	@PUT
	@Path("/{groupName}")
	public String updateSpaceQuota(@Context HttpServletRequest request,
			@PathParam("groupName") String groupName) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		InputStream in = request.getInputStream();
		String requestFilePath = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "updateStorageSpace/"
				+ groupName + "_" + serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> resultParseRequest = parseRequest(requestFilePath); // 解析xml请求参数
		if (!"".equals(resultParseRequest.get("error"))) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(resultParseRequest.get("error")));
		}
		
		String groupPath = InitConfig.ROOT_PATH + groupName;
		if (!hdfsUtil.existFile(groupPath)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the path '" + groupPath.toString() + "' is not exist")); // 返回错误信息
		}
		setSpaceQuota(groupName, resultParseRequest.get("quota")); // 设定配额
		String theQuota = hdfsUtil.getSpaceQuota(groupPath);
		
		Map<String, String> mapsuc = new HashMap<String, String>(); 
		mapsuc.put("quota", theQuota);
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}

	/**
	 * 删除存储单元（删除数据库、删除目录）
	 * 
	 * @DELETE 响应DELETE请求
	 * @Path 定义路径
	 * @param groupName 存储单元名
	 * @return 返回删除信息
	 * @throws Exception  抛出异常
	 */
	@DELETE
	@Path("/{groupName}")
	public String deleteStorageSpace(@PathParam("groupName") String groupName) throws Exception {	
		if (!hiveUtil.existDB("tmp_" + groupName)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the database 'tmp_" + groupName + "' is not exist"));
		}
		if (!hiveUtil.existDB("result_" + groupName)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the database 'result_" + groupName + "' is not exist"));
		}
		String dirPath = InitConfig.ROOT_PATH + groupName;
		if (!hdfsUtil.existFile(dirPath)) {
			return "the hdfs dir '" + dirPath + "' is not exist"; 
		} 
		
		hiveUtil.dropDB("tmp_" + groupName);
		hiveUtil.dropDB("result_" + groupName);
		hdfsUtil.deleteFile(dirPath);
		
		return JavaUtil.createXml1("response", JavaUtil.errMap(""));
	}

	
	/**
	 * 创建存储目录
	 * 
	 * @param groupName 组名
	 * @throws Exception  抛出异常
	 */
	private String createDir(String groupName) throws Exception {
		String tmpdb =  InitConfig.ROOT_PATH + groupName + "/tmp_" + groupName;
		String tmpfile = InitConfig.ROOT_PATH + groupName + "/tmp_dir";
		String resultdb = InitConfig.ROOT_PATH + groupName + "/result_" + groupName;
		String resultfile = InitConfig.ROOT_PATH + groupName + "/result_dir";
		hdfsUtil.mkdirs(tmpdb); // 创建目录
		hdfsUtil.mkdirs(tmpfile);
		hdfsUtil.mkdirs(resultdb);
		hdfsUtil.mkdirs(resultfile);
		return "ok";
	}

	/**
	 * 创建临时、结果数据库
	 * 
	 * @param groupName
	 * @return 返回创建状态
	 * @throws Exception 抛出异常
	 */
	private void createDB(String groupName) throws Exception {
		String dbNameTmp = "tmp_" + groupName;
		String localUriTmp = InitConfig.ROOT_PATH + groupName + "/tmp_"+ groupName;
		hiveUtil.createDB(dbNameTmp, localUriTmp);
		String dbNameResult = "result_" + groupName;
		String localUriResult = InitConfig.ROOT_PATH + groupName + "/result_"+ groupName;
		hiveUtil.createDB(dbNameResult, localUriResult);
	}
	
	
	/**
	 * 解析创建、修改配额请求文件
	 * 
	 * @param xmlFilePath 请求文件本地保存路径
	 * @return 返回解析请求结果
	 * @throws Exception 抛出异常
	 */
	private Map<String, String> parseRequest(String xmlFilePath) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("error", "");
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		Element rootElement = document.getRootElement(); // 获得根节点
		if (rootElement.selectSingleNode("quota") != null) {
			String quota = rootElement.selectSingleNode("quota").getText();
			Long quota_long = Long.parseLong(quota.substring(0, quota.length() - 2)) * 1024 * 1024 * 1024;
			if (quota_long <= InitConfig.MAXQUOTA) {
				map.put("quota", quota_long.toString());
			} else {
				map.put("error", "quota can not larger than " + String.valueOf(InitConfig.MAXQUOTA));
			}		
		} else {
			map.put("error", "quota is null");
			return map;
		}
		return map;
	}

	/**
	 * 设定存储目录配额
	 * 
	 * @param groupName 组名
	 */
	private void setSpaceQuota(String groupName, String quota) throws Exception {
		String dirPath = InitConfig.ROOT_PATH + groupName;
		hdfsUtil.setSpaceQuota(dirPath, quota);
	}
		
}
