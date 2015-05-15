package iie.ihadoop.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import iie.ihadoop.db.OperatorDAO;
import iie.ihadoop.model.Operator;
import iie.ihadoop.util.HdfsUtil;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;

/**
 * 算子管理模块
 * 
 * @author sunyang
 *
 */
@Path("/operators")
public class Operators {
	OperatorDAO operatorDao = new OperatorDAO();
	Operator operator = new Operator();
	HdfsUtil hdfsUtil = new HdfsUtil();
	
	/**
	 * 注册算子
	 * @param request 请求
	 * @throws Exception 抛出异常
	 */
	@POST
	public String registerOperator(@Context HttpServletRequest request)throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳

		InputStream in = request.getInputStream();
		String requestFilePath = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "registerOp"
		         + "/" + serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath); // 将接收的xml配置信息保存到文件
		in.close();
		String option = "regist";
		String resultParseRequest = parseRequest(requestFilePath, option); // 解析xml请求参数
		if (!"ok".equals(resultParseRequest)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(resultParseRequest)); // 生成xml响应
		}

		// 查看数据库中是否存在该opid
		operator.setRegisterTime(serialName.substring(0, 19));
		String opid = operator.getOpid();
		String hdfsZipPath = operator.getOpjarpath();
		boolean existOpid = operatorDao.existOpid(opid);
		if (existOpid) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opid '" + opid + "' is exist")); // 生成xml响应
		}
		String checkZipResult = checkZip(hdfsZipPath, opid);
		if (!"ok".equals(checkZipResult)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(checkZipResult));
		}
		if(!"ok".equals(checkOpetator())) { //检验请求参数值长度
			return JavaUtil.createXml1("error", JavaUtil.errMap(checkOpetator()));
		}
		
		operatorDao.insertOperator(operator);// 向数据库中插入数据
		createChecksum(hdfsZipPath);// 在hdfs生成时间戳校验码

		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("opid", opid);
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}

	/**
	 * 算子更新
	 * 
	 * @param request 请求
	 * @return
	 * @throws Exception 抛出异常
	 */
	@PUT
	@Path("/{opid}")
	public String updateOperator(@Context HttpServletRequest request, @PathParam("opid") String opid) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳

		InputStream in = request.getInputStream();
		String requestFilePath = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "updateOp"
				 + "/" + serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath); // 将接收的xml配置信息保存到文件
		in.close();
		

		// 查看数据库中是否存在该opid;
		boolean existOpid = operatorDao.existOpid(opid);
		if (!existOpid) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opid '" + opid + "' is not exist")); // 生成xml响应
		}
		
		String option = "update";
		String resultParseRequest = parseRequest(requestFilePath, option); // 解析xml请求参数
		if (!"ok".equals(resultParseRequest)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(resultParseRequest)); // 生成xml响应
		}

		String hdfsZipPath = operator.getOpjarpath();
		String checkZipResult = checkZip(hdfsZipPath, opid);
		if (!"ok".equals(checkZipResult)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(checkZipResult));
		}
		
		operator.setEnginename(opid.substring(0, opid.indexOf(".")));
		operator.setRegisterTime(serialName.substring(0, 18));
		if(!"ok".equals(checkOpetator())) { //检验请求参数值长度
			return JavaUtil.createXml1("error", JavaUtil.errMap(checkOpetator()));
		}
		operatorDao.updateOperator(operator); //更新数据库
		createChecksum(hdfsZipPath); //hdfs生成校验码

		return JavaUtil.createXml1("response", JavaUtil.errMap(""));
	}

	/**
	 * 删除算子
	 * 
	 * @param opid 算子id
	 * @return 返回删除结果
	 * @throws Exception 抛出异常
	 */
	@DELETE
	@Path("/{opid}")
	public String deleteOpertor(@PathParam("opid") String opid)
			throws Exception {
		boolean existOpid = operatorDao.existOpid(opid); //查看数据库是否存在该opid
		if (!existOpid) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opid '" + opid + "' is not exist")); // 生成xml响应
		}

		String opjarPath = operatorDao.getOpjarPath(opid); //删除HDFS上checksum文件
		deleteChecksum(opjarPath, opid);
		
		operatorDao.deleteOperator(opid); //删除数据库数据

		return JavaUtil.createXml1("response", JavaUtil.errMap(""));
	}

	private String checkZip(String hdfsZipPath, String opid) throws Exception {
		String result = "";
		String opZipTemp = InitConfig.BASEPATH + "temp" + hdfsZipPath.substring(hdfsZipPath.lastIndexOf("/"));
		hdfsUtil.getFileFromHdfs(hdfsZipPath, opZipTemp);
		
		ZipFile zipFile = new ZipFile(opZipTemp, Charset.forName("GBK"));
		Enumeration<? extends ZipEntry> e = zipFile.entries();
		List<String> subList = new ArrayList<String>();
		while (e.hasMoreElements()) {
			ZipEntry zipElement = (ZipEntry) e.nextElement();
			String fileName = zipElement.getName();
			subList.add(fileName);
			System.out.println("算子包中的目录结构为=======" + fileName);
		}
		if (!subList.contains(opid + "/run.sh") || !subList.contains(opid + "/" + opid+".jar")
				|| !subList.contains(opid + "/description.xml") || !subList.contains(opid + "/ui.xml")) {
			System.out.println("The ZIP file of '" + opid + "' in HDFS  is not completed .");
			JavaUtil.deleteDir(opZipTemp);
			zipFile.close();
			result = "The ZIP file of '" + opid + "' in HDFS  is not completed .";
			return result;
		}
		if ("GM".equals(operator.getEnginename())) { //单机算子要检验是否使用DShell提交
			boolean existDShellJar = false;
			for (int i = 0; i < subList.size(); i++) { //是否存在DShell.jar包，Dshell2.5.0V2.jar
				String fileName = subList.get(i);
				String regEx = "Dshell[0-9]\\.[0-9]\\.[0-9]V[0-9]\\.jar$";
				Pattern pat = Pattern.compile(regEx);
				Matcher mat = pat.matcher(fileName);
				if (mat.find()) {
					existDShellJar = true;
					break;
				}
			}
			//run.sh脚本中是否使用DShell.jar包
			boolean useDShellJar = false;
			ZipInputStream zin = new ZipInputStream(new FileInputStream(opZipTemp), Charset.forName("GBK"));
			ZipEntry ze;
			while ((ze = zin.getNextEntry()) != null) {
				System.out.println(ze.getName());
				if (!(opid + "/run.sh").equals(ze.getName()))
					continue;
				long size = ze.getSize(); 
				if (size > 0) {
					BufferedReader br = new BufferedReader(new InputStreamReader(zipFile.getInputStream(ze)));
					String line;
					String regEx = "[\\s\\S]*Dshell[0-9]\\.[0-9]\\.[0-9]V[0-9]\\.jar[\\s\\S]*$"; //所有空字符+所有非空字符
					Pattern pat = Pattern.compile(regEx);
					while ((line = br.readLine()) != null) {
						if (line.trim().startsWith("#"))
							continue;
						Matcher mat = pat.matcher(line);
						if (mat.find()) {
							useDShellJar = true;
						}
					}
					br.close();
				}
			}
			zin.close();

			if (!existDShellJar || !useDShellJar) {
				System.out.println("该单机算子中没有DShell包或者没有run.sh中没有使用DShell包");
				JavaUtil.deleteDir(opZipTemp);
				zipFile.close();
				result = "该单机算子需要使用DShell提交到YARN执行";
				return result;
			}
		} 
	         
	    zipFile.close();
		JavaUtil.deleteDir(opZipTemp);//删除本地文件
		return "ok";
	}
	
	private String checkOpetator() {
		if(operator.getOpid().length() > 64) {
			return "opid长度不能大于64";
		}
		if(operator.getDevlang().length() > 64) {
			return "devlang长度不能大于64";
		}
		if(operator.getVersion().length() > 64) {
			return "version长度不能大于64";
		}
		if(operator.getProvider().length() > 64) {
			return "provider长度不能大于64";
		}
		if(operator.getEnginename().length() > 64) {
			return "enginename长度不能大于64";
		}
		if(operator.getOpjarpath().length() > 64) {
			return "opjarpath长度不能大于64";
		}
		if(operator.getDescriptor().length() > 1024) {
			return "descriptor长度不能大于1024";
		}
		if(operator.getInstruction().length() > 1024) {
			return "instruction长度不能大于1024";
		}
		return "ok";
	}
	
	/**
	 * 在hdfs上生成校验码（验证hdfs的算子与本地算子是否一致）
	 * @param hdfsZipPath 算子zip包hdfs路径
	 * @throws Exception 抛出异常
	 */
	private void createChecksum(String hdfsZipPath) throws Exception {
		String opjarParent = hdfsZipPath.substring(0, operator.getOpjarpath().lastIndexOf("/") + 1);
		String checksumHdfsPath = opjarParent + InitConfig.CHECKSUM_DIR + operator.getOpid() + ".txt";
		String checksumData = operator.getRegisterTime();
		hdfsUtil.putStringToHdfs(checksumData, checksumHdfsPath);
	}
	
	/**
	 * 删除hdfs上 的校验码
	 * @param opZipParent hdfs上算子压缩包的父目录
	 * @throws Exception 抛出异常
	 */
	private void deleteChecksum(String hdfsZipPath, String opid) throws Exception {
		String opjarParent = hdfsZipPath.substring(0, hdfsZipPath.lastIndexOf("/") + 1);
		String checksumHdfsPath = opjarParent + InitConfig.CHECKSUM_DIR + opid + ".txt";
		hdfsUtil.deleteFile(checksumHdfsPath);
	}
	
	/**
	 * 解析xml请求文件
	 * 
	 * @param xmlFilePath xml配置文件路径
	 * @param option 解析哪个方法请求(注册/更新)
	 */
	private String parseRequest(String xmlFilePath, String option) throws Exception {
		boolean existOpid = false;
		boolean existOpjarpath = false;
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if ("opid".equals(node2.attributeValue("name")) && "regist".equals(option)) {
				existOpid = true;
				String opid = node2.attributeValue("value");
				if ("".equals(opid) ) {
					return "opid is null";
				}
				if (opid.contains(" ")) {
					return "operator name contains space";
				}
				if (!opid.contains(".")) {
					return "operator name should contains '.'";
				}
				String engineName = opid.substring(0, opid.indexOf("."));
				if (!"GM".equals(engineName) && !"SK".equals(engineName) &&
						!"MR".equals(engineName) && !"HV".equals(engineName)) {
					return "operator name should begin with GM,SK,MR,HV";
				}
				operator.setEnginename(engineName);
				operator.setOpid(opid);
			}
			if ("version".equals(node2.attributeValue("name"))) {
				operator.setVersion(node2.attributeValue("value"));		
			}
			if ("biztype".equals(node2.attributeValue("name"))) {
				operator.setBiztype(node2.attributeValue("value"));		
			}
			if ("provider".equals(node2.attributeValue("name"))) {
				operator.setProvider(node2.attributeValue("value"));
			}
			if ("opjarpath".equals(node2.attributeValue("name"))) {
				existOpjarpath = true;
				String opjarpath = node2.attributeValue("value");
				if ("".equals(opjarpath)) {
					return "opjarpath is null";
				}
				if (!hdfsUtil.existFile(opjarpath)) {
					return "the hdfs zip(" + opjarpath + ") is not exist";
				}
				operator.setOpjarpath(opjarpath);
			}
			if ("devlang".equals(node2.attributeValue("name"))) {
				operator.setDevlang(node2.attributeValue("value"));
			}
			if ("descriptor".equals(node2.attributeValue("name"))) {
				operator.setDescriptor(node2.attributeValue("value"));
			}
			if ("instruction".equals(node2.attributeValue("name"))) {
                operator.setInstruction(node2.attributeValue("value"));
			}
		}
		if (existOpid == false && "regist".equals(option)) {
			return "opid is null";
		}
		if (existOpjarpath == false) {
			return "opjarpath is null";
		}
		return "ok";
	}
	
	
//	@Path("/regist")
//	@POST
//	public String copyToWorkPath(@Context HttpServletRequest request)
//			throws Exception {
//		InputStream in = request.getInputStream();
//		String requestString = Myutil.convertStreamToString(in);
//		String[] conf = requestString.split("&");
//
//		String[] cmd = new String[] { "/bin/bash", "/usr/local/ops/register.sh" };
//		String[] envp = new String[] { "opName=" + conf[0].split("=")[1],
//				"opIP=" + conf[1].split("=")[1],
//				"opPath=" + conf[2].split("=")[1] }; // 给脚本传入参数
//		File workdir = new File("/usr/local/ops/");
//		Process p = Runtime.getRuntime().exec(cmd, envp, workdir); // 执行register.sh
//		p.waitFor();
//
//		InputStream in2 = p.getInputStream();
//		String shOutString = Myutil.convertStreamToString(in2);
//		System.out.println("脚本中的输出为：" + shOutString);
//		in2.close();
//		in.close();
//		return "ok";
//	}

}
