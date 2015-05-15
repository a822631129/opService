package iie.ihadoop.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import iie.ihadoop.db.ApplicationDAO;
import iie.ihadoop.db.HiveInstanceDAO;
import iie.ihadoop.db.OpTaskDAO;
import iie.ihadoop.db.OperatorDAO;
import iie.ihadoop.model.ApplicationInstance;
import iie.ihadoop.model.OpTask;
import iie.ihadoop.util.HdfsUtil;
import iie.ihadoop.util.HiveUtil;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;
import iie.ihadoop.util.YarnUtil;

/**
 * 算子任务管理模块
 * 
 * @author sunyang
 *
 */
@Path("/optasks")
public class OpTasks extends Thread {
	OpTaskDAO opTaskDao = new OpTaskDAO();
	ApplicationDAO appDao = new ApplicationDAO();
	YarnUtil yarnUtil = new YarnUtil();
	HdfsUtil hdfsUtil = new HdfsUtil();

	/**
	 * 描述：接收请求同步调用算子，返回stdout.xml内容。 实现流程： 0.收到请求，验证算子是否已经注册 1.获取请求体内容，写到本地文件
	 * 2.解析请求参数 3.将请求体的内容写到HDFS 4.设置执行算子时的环境变量
	 * 5.检查本地是否有算子和检验码，若是任何一个没有就下载解压算子和检验码
	 * 6.若是有本地算子和校验码，比较本地校验码与hdfs的校验码的值是否一样，不一样就下载解压算子和校验码 7.创建进程，执行算子的run.sh脚本
	 * 8.监测保存算子的执行日志信息 若是hive算子就在进程执行结束后，查看算子任务状态，保存到数据库
	 * 若是其他算子，将在进程的流信息中提取出算子任务产生的Application保存到数据库记录状态，当产生新的
	 * 新的Application，就查上一个Application的状态，更新到数据库，当流结束时，检查最后一个Application状态
	 * 更新到数据库。 9.获取算子的输出文件stdout.xml(若是没有就获取stderr文件)
	 * 10.查看数据库算子任务表，查看该算子任务是否被killed，如果killed，返回相应信息
	 * 11.若是hdfs上有stdout.xml文件或者stderr文件，就返回其内容，没有返回错误提示信息
	 * 
	 * @POST 响应POST请求
	 * @Path 定义路径
	 * @Context 注入上下文对象
	 * @PathParam 路径参数
	 * @param request 请求
	 * @param opid 算子id
	 */
	@POST
	@Path("/{opid}/sync")
	public String submitOpTaskSync(@Context HttpServletRequest request,// 同步算子
			@PathParam("opid") String opid, @QueryParam("sync") String sync) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		String otid = opid + serialName; // 定义算子任务id
		String response = "";
		String stdoutPath = InitConfig.STDOUT + serialName.substring(0, 10)
				+ "/" + opid + "_" + serialName.substring(11) + ".txt";
		JavaUtil.log("接收到请求：POST /optasks/" + opid + "/sync", stdoutPath);
		OperatorDAO opdao = new OperatorDAO();
		if (!opdao.existOpid(opid)) {
			JavaUtil.log("申请提交的算子 '" + opid + "'还没有注册", stdoutPath);
			return JavaUtil.createXml1("error", JavaUtil.errMap("申请提交的算子 '" + opid + "'还没有注册"));
		}
		JavaUtil.log("请求体内容：", stdoutPath);
		InputStream ins = request.getInputStream();
		String xmlFilePath = InitConfig.REQUESTFILEPATH + serialName.substring(0, 10) + "/" + opid + "_"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(ins, xmlFilePath); // 将接收的xml配置信息保存到本地文件
		ins.close();
		InputStream is = new FileInputStream(new File(xmlFilePath));
		JavaUtil.writeInputStreamToFile(is, stdoutPath);
		is.close();
		JavaUtil.log("\n" + JavaUtil.getTime() + "开始解析请求体", stdoutPath);
		Map<String, String> mapParseXmlFile = parseRequest(xmlFilePath); // 解析xml请求参数
		if (!"".equals(mapParseXmlFile.get("error"))) {
			response = mapParseXmlFile.get("error");
			JavaUtil.log(response, stdoutPath);
			return JavaUtil.createXml1("error", JavaUtil.errMap(response));
		} else {
			JavaUtil.log("解析请求体成功：", stdoutPath);
		}
		JavaUtil.log("开始将请求体信息上传到HDFS上", stdoutPath);
		String tempHdfsBasePath = mapParseXmlFile.get("tempHdfsBasePath");
		String CONF = "CONF=" + fileToHdfs(xmlFilePath, tempHdfsBasePath); // 将xml配置信息文件保存到HDFS
		JavaUtil.log("成功将请求体信息上传到HDFS上", stdoutPath);
		JavaUtil.log("开始设置环境变量：", stdoutPath);
		String runPath = InitConfig.BASEPATH + opid + "/run.sh";
		File runFile = new File(runPath);

		String opjarPathLocal = InitConfig.BASEPATH + opid + "/" + opid + ".jar";
		String OPJAR = "OPJAR=" + opjarPathLocal;
		String LIBJARS = getLIBJARS(opid);
		String QUEUE = mapParseXmlFile.get("QUEUE");
		String USER = mapParseXmlFile.get("USER");
		String PROCID = mapParseXmlFile.get("PROCID");
		String PIID = mapParseXmlFile.get("PIID");
		File workdir = new File(InitConfig.BASEPATH + opid);
		String WORKDIR = "WORKDIR=" + InitConfig.BASEPATH + opid;
		String[] cmd = new String[] { "/bin/bash", runPath };
		String[] envp = setEnvp(otid, CONF, OPJAR, LIBJARS, QUEUE, USER, opid,
				PROCID, PIID, WORKDIR, stdoutPath);

		JavaUtil.log(runPath, stdoutPath);
		JavaUtil.log(OPJAR, stdoutPath);
		JavaUtil.log(LIBJARS, stdoutPath);
		JavaUtil.log(QUEUE, stdoutPath);
		JavaUtil.log(USER, stdoutPath);
		JavaUtil.log(PROCID, stdoutPath);
		JavaUtil.log(PIID, stdoutPath);

		String localChecksumPath = InitConfig.BASEPATH + InitConfig.CHECKSUM_DIR + opid + ".txt";
		String localOpPath = InitConfig.BASEPATH + opid;
		String opjarPathHdfs = opdao.getOpjarPath(opid);
		//如果不存在本地算子或者校验码就下载算子
		if (!JavaUtil.existLocalFile(localChecksumPath) || !JavaUtil.existLocalFile(localOpPath)) {
			JavaUtil.deleteDir(localOpPath); // 删除本地算子
			downloadOpFromHdfs(opid, opjarPathHdfs); //从hdfs下载算子zip和校验码到缓存路径
			unzipTemp(opid, stdoutPath); //解压zip到算子目录
		} else if (!isEqualChecksum(opid, opjarPathHdfs)) {
			JavaUtil.deleteDir(localOpPath);
			downloadOpFromHdfs(opid, opjarPathHdfs); //从hdfs下载算子zip和校验码到缓存路径
			unzipTemp(opid, stdoutPath); //解压zip到算子目录
		}
		
		if (!runFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the run.sh path '" + runPath + "' is not exist"));
		}
		File opjarFile = new File(opjarPathLocal);
		if (!opjarFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opjar path '" + opjarPathLocal + "' is not exist"));
		}

		JavaUtil.log("	开始执行run.sh,提交算子任务", stdoutPath);
		Process p = Runtime.getRuntime().exec(cmd, envp, workdir); // 执行run.sh

		recordOpTask(otid, opid, QUEUE.substring(6), USER.substring(5),
				PROCID.substring(7), PIID.substring(5), serialName, "sync");

		JavaUtil.log("获取执行算子的屏幕输出信息(ErrorStream)，可检查是否成功提交算子任务,"
				+ "没有屏幕输出信息或者不成功请检查上文请求体信息以及设置的环境变量是否正确", stdoutPath);
		String engineName = opid.substring(0, opid.indexOf("."));
		if ("HV".equals(engineName)) {
			trackTaskSync_hive(p, opid, otid, serialName, stdoutPath);
		} else {
			trackTaskSync(p, opid, otid, serialName, stdoutPath);
		}
		JavaUtil.log("ErrorStream结束", stdoutPath);
		JavaUtil.log("获取run.sh的输出信息(InputStream)，(比如run.sh中的echo $OPJAR)可检查是否执行run.sh脚本", stdoutPath);
		getInputStream(p, stdoutPath); // 获得进程InputStream信息，保存到文件
		JavaUtil.log("InputStream结束", stdoutPath);
		p.waitFor();
		JavaUtil.log("算子任务执行结束", stdoutPath);

		if (isOpTaskKilled(otid, stdoutPath)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the optask is killed"));
		}

		JavaUtil.log("获取" + mapParseXmlFile.get("tempHdfsBasePath") + "目录下的stdout.xml内容", stdoutPath);
		response = getStdoutFromHdfs(mapParseXmlFile.get("tempHdfsBasePath"));
		JavaUtil.log(response, stdoutPath);
		return response;
	}

	/**
	 * 描述：接收请求异步调用算子，返回stdout.xml内容。 实现流程： 1.获取请求体内容，写到本地文件 2.解析请求参数
	 * 3.将请求体的内容写到HDFS 4.设置执行算子时的环境变量 5.检查本地是否有算子和检验码，若是任何一个没有就下载解压算子和检验码
	 * 6.若是有本地算子和校验码，比较本地校验码与hdfs的校验码的值是否一样，不一样就下载解压算子和校验码 7.创建个进程执行算子的run.sh脚本
	 * 8.创建新线程，监测保存算子的执行日志信息，以至于达到异步提交算子任务的效果 若是hive算子就在进程执行结束后，查看算子任务状态，保存到数据库
	 * 若是其他算子，将在进程的流信息中提取出算子任务产生的Application保存到数据库记录状态，当产生新的
	 * 新的Application，就查上一个Application的状态，更新到数据库，当流结束时，检查最后一个Application状态
	 * 更新到数据库。 9.返回算子任务id
	 * 
	 * @POST 响应POST请求
	 * @Path 定义路径
	 * @Context 注入上下文对象
	 * @PathParam 路径参数
	 * @param request 请求
	 * @param opid 算子id
	 */
	@POST
	@Path("/{opid}/async")
	public String submitOpTaskAsync(@Context HttpServletRequest request, @PathParam("opid") String opid) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		String otid = opid + serialName; // 定义算子任务id
		String response = otid;

		String stdoutPath = InitConfig.STDOUT + serialName.substring(0, 10)
				+ "/" + opid + "_" + serialName.substring(11) + ".txt";
		JavaUtil.log("接收到请求：POST /optasks/" + opid + "/async", stdoutPath);
		OperatorDAO opdao = new OperatorDAO();
		if (!opdao.existOpid(opid)) {
			JavaUtil.log("申请提交的算子 '" + opid + "'还没有注册", stdoutPath);
			return JavaUtil.createXml1("error", JavaUtil.errMap("申请提交的算子 '" + opid + "'还没有注册"));
		}
		JavaUtil.log("请求体内容：", stdoutPath);

		InputStream ins = request.getInputStream();
		String xmlFilePath = InitConfig.REQUESTFILEPATH + serialName.substring(0, 10) + "/" + opid + "_"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(ins, xmlFilePath); // 将接收的xml配置信息保存到本地文件
		ins.close();
		InputStream is = new FileInputStream(new File(xmlFilePath));
		JavaUtil.writeInputStreamToFile(is, stdoutPath);
		is.close();
		JavaUtil.log("开始解析请求体", stdoutPath);
		Map<String, String> mapParseXmlFile = parseRequest(xmlFilePath); // 解析xml请求参数
		if (!"".equals(mapParseXmlFile.get("error"))) {
			response = mapParseXmlFile.get("error");
			JavaUtil.log(response, stdoutPath);
			return JavaUtil.createXml1("error", JavaUtil.errMap(response));
		} else {
			JavaUtil.log("解析请求体成功：", stdoutPath);
		}
		JavaUtil.log("开始将请求体信息上传到HDFS上", stdoutPath);
		String tempHdfsBasePath = mapParseXmlFile.get("tempHdfsBasePath");
		String CONF = "CONF=" + fileToHdfs(xmlFilePath, tempHdfsBasePath); // 将xml配置信息文件保存到HDFS
		JavaUtil.log("成功将请求体信息上传到HDFS上", stdoutPath);
		JavaUtil.log("开始设置环境变量：", stdoutPath);
		String runPath = InitConfig.BASEPATH + opid + "/run.sh";

		String opjarPathLocal = InitConfig.BASEPATH + opid + "/" + opid + ".jar";
		String OPJAR = "OPJAR=" + opjarPathLocal;

		String LIBJARS = getLIBJARS(opid);
		String QUEUE = mapParseXmlFile.get("QUEUE");
		String USER = mapParseXmlFile.get("USER");
		String PROCID = mapParseXmlFile.get("PROCID");
		String PIID = mapParseXmlFile.get("PIID");
		File workdir = new File(InitConfig.BASEPATH + opid);
		String WORKDIR = "WORKDIR=" + InitConfig.BASEPATH + opid;
		String[] cmd = new String[] { "/bin/bash", runPath };
		String[] envp = setEnvp(otid, CONF, OPJAR, LIBJARS, QUEUE, USER, opid, PROCID, PIID, WORKDIR, stdoutPath);

		JavaUtil.log(runPath, stdoutPath);
		JavaUtil.log(OPJAR, stdoutPath);
		JavaUtil.log(LIBJARS, stdoutPath);
		JavaUtil.log(QUEUE, stdoutPath);
		JavaUtil.log(USER, stdoutPath);
		JavaUtil.log(PROCID, stdoutPath);
		JavaUtil.log(PIID, stdoutPath);

		String localChecksumPath = InitConfig.BASEPATH + InitConfig.CHECKSUM_DIR + opid + ".txt";
		String localOpPath = InitConfig.BASEPATH + opid;
		String opjarPathHdfs = opdao.getOpjarPath(opid);
		//如果不存在本地算子或者校验码就下载算子
		if (!JavaUtil.existLocalFile(localChecksumPath) || !JavaUtil.existLocalFile(localOpPath)) {
			JavaUtil.deleteDir(localOpPath); // 删除本地算子
			downloadOpFromHdfs(opid, opjarPathHdfs); //从hdfs下载算子zip和校验码到缓存路径
			unzipTemp(opid, stdoutPath); //解压zip到算子目录
		} else if (isEqualChecksum(opid, opjarPathHdfs)) {
			JavaUtil.deleteDir(localOpPath);
			downloadOpFromHdfs(opid, opjarPathHdfs); //从hdfs下载算子zip和校验码到缓存路径
			unzipTemp(opid, stdoutPath); //解压zip到算子目录
		}
		File runFile = new File(runPath);
		if (!runFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the run.sh path '" + runPath
							+ "' is not exist"));
		}
		File opjarFile = new File(opjarPathLocal);
		if (!opjarFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opjar path '" + opjarPathLocal + "' is not exist"));
		}

		JavaUtil.log("开始执行run.sh,提交算子任务", stdoutPath);
		Process p = Runtime.getRuntime().exec(cmd, envp, workdir); // 执行run.sh

		recordOpTask(otid, opid, QUEUE.substring(6), USER.substring(5),
				PROCID.substring(7), PIID.substring(5), serialName, "async");
		String engineName = opid.substring(0, opid.indexOf("."));
		if ("HV".equals(engineName)) { // 需要确定是否是HIVE这个词
			trackTaskAsync_hive(p, opid, otid, serialName, stdoutPath);
		} else {
			trackTaskAsync(p, opid, otid, serialName, stdoutPath); // 获得进程ErrorStream信息,提取出applicationId，保存到文件
		}
		return otid;
	}

	/**
	 * 原调试接口，无需注册，直接执行算子
	 * @param request 请求
	 * @param opid 算子id
	 * @param sync 同步
	 */
	@POST
	@Path("/{opid}")
	public String submitOpTaskNoRegister(@Context HttpServletRequest request,// 中心联调，同步提交算子任务使用接口
			@PathParam("opid") String opid, @QueryParam("sync") String sync) throws Exception {
		String response = "";
		String serialName = JavaUtil.getTime(); // 获取时间戳
		String otid = opid + serialName; // 定义算子任务id
		String stdoutPath = InitConfig.STDOUT + serialName.substring(0, 10)
				+ "/" + opid + "_" + serialName.substring(11) + ".txt";
		JavaUtil.log("接收到请求：POST /optasks/" + opid, stdoutPath);
		JavaUtil.log("请求体内容：", stdoutPath);

		InputStream ins = request.getInputStream();
		String xmlFilePath = InitConfig.REQUESTFILEPATH + serialName.substring(0, 10) + "/" + opid + "_"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(ins, xmlFilePath); // 将接收的xml配置信息保存到本地文件
		ins.close();
		InputStream is = new FileInputStream(new File(xmlFilePath));
		JavaUtil.writeInputStreamToFile(is, stdoutPath);
		is.close();
		JavaUtil.log("开始解析请求体", stdoutPath);
		Map<String, String> mapParseXmlFile = parseRequest(xmlFilePath); // 解析xml请求参数
		if (!"".equals(mapParseXmlFile.get("error"))) {
			response = mapParseXmlFile.get("error");
			JavaUtil.log(response, stdoutPath);
			return JavaUtil.createXml1("error", JavaUtil.errMap(response));
		} else {
			JavaUtil.log("解析请求体成功：", stdoutPath);
		}
		JavaUtil.log("开始将请求体信息上传到HDFS上", stdoutPath);
		String tempHdfsBasePath = mapParseXmlFile.get("tempHdfsBasePath");
		String CONF = "CONF=" + fileToHdfs(xmlFilePath, tempHdfsBasePath); // 将xml配置信息文件保存到HDFS
		JavaUtil.log("成功将请求体信息上传到HDFS上", stdoutPath);
		JavaUtil.log("开始设置环境变量：", stdoutPath);
		String runPath = InitConfig.BASEPATH + opid + "/run.sh";
		File runFile = new File(runPath);
		if (!runFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the run.sh path '" + runPath + "' is not exist"));
		}
		String opjarPathLocal = InitConfig.BASEPATH + opid + "/" + opid + ".jar";
		String OPJAR = "OPJAR=" + opjarPathLocal;
		File opjarFile = new File(opjarPathLocal);
		if (!opjarFile.exists()) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the opjar path '" + opjarPathLocal + "' is not exist"));
		}
		String LIBJARS = getLIBJARS(opid);
		String QUEUE = mapParseXmlFile.get("QUEUE");
		String USER = mapParseXmlFile.get("USER");
		String PROCID = mapParseXmlFile.get("PROCID");
		String PIID = mapParseXmlFile.get("PIID");
		File workdir = new File(InitConfig.BASEPATH + opid);
		String WORKDIR = "WORKDIR=" + InitConfig.BASEPATH + opid;
		String[] cmd = new String[] { "/bin/bash", runPath };
		String[] envp = setEnvp(otid, CONF, OPJAR, LIBJARS, QUEUE, USER, opid, PROCID, PIID, WORKDIR, stdoutPath);

		JavaUtil.log(runPath, stdoutPath);
		JavaUtil.log(OPJAR, stdoutPath);
		JavaUtil.log(LIBJARS, stdoutPath);
		JavaUtil.log(QUEUE, stdoutPath);
		JavaUtil.log(USER, stdoutPath);
		JavaUtil.log(PROCID, stdoutPath);
		JavaUtil.log(PIID, stdoutPath);

		JavaUtil.log("开始执行run.sh,提交算子任务", stdoutPath);
		Process p = Runtime.getRuntime().exec(cmd, envp, workdir); // 执行run.sh
		JavaUtil.log("获取run.sh的输出信息(InputStream)，(比如run.sh中的echo $OPJAR)可检查是否执行run.sh脚本", stdoutPath);
		getInputStream(p, stdoutPath); // 获得进程InputStream信息，保存到文件
		JavaUtil.log("InputStream结束", stdoutPath);
		JavaUtil.log("获取执行算子的屏幕输出信息(ErrorStream)，可检查是否成功提交算子任务,"
				+ "没有屏幕输出信息或者不成功请检查上文请求体信息以及设置的环境变量是否正确", stdoutPath);
		trackTaskNoRegister(p, opid, serialName, stdoutPath);
		JavaUtil.log("ErrorStream结束", stdoutPath);
		p.waitFor();
		JavaUtil.log("算子任务执行结束", stdoutPath);
		JavaUtil.log("获取" + mapParseXmlFile.get("tempHdfsBasePath") + "目录下的stdout.xml内容", stdoutPath);
		response = getStdoutFromHdfs(mapParseXmlFile.get("tempHdfsBasePath"));
		JavaUtil.log(response, stdoutPath);
		return response;
	}

	/**
	 * 查看算子任务状态
	 * 
	 * @param otid 算子任务id
	 * @GET 响应GET请求
	 * @Path 定义路径
	 * @return 返回算子任务状态和任务提交时间
	 */
	@GET
	@Path("/{otid}")
	public String getOpTaskStatus(@PathParam("otid") String otid) throws Exception {
		List<String> list = opTaskDao.getOptaskStatus(otid); //向业务数据库查询算子任务状态
		String status = list.get(0);
		String submittime = list.get(1);
		if ("".equals(status)) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the optask '" + otid + "' is not exist"));
		}
		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("status", status);
		mapsuc.put("submittime", submittime);
		return JavaUtil.createXml1("response", mapsuc); // 生成xml响应
	}

	/**
	 * 结束异步提交的算子任务
	 * @param otid 算子任务id
	 * @DELETE 响应DELETE请求
	 * @Path 定义路径
	 * @return 返回算 子任务状态和任务提交时间
	 */
	@DELETE
	@Path("/{otid}")
	public String killOpTaskAsync(@PathParam("otid") String otid) throws Exception {
		String engineName = otid.substring(0, otid.indexOf("."));
		List<String> opTask = opTaskDao.getOptaskStatus(otid);
		if (opTask.size() == 0) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the optask '" + otid + "' is not exist")); // 生成xml响应
		}
		if (!"RUNNING".equals(opTask.get(0))) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the optask '" + otid + "' is FINISHED")); // 生成xml响应
		}
		if ("HV".equals(engineName)) { //结束hive算子任务
			HiveUtil hu = new HiveUtil(otid);
			hu.killHiveOp();
		} else { //结束MR、SK、GM算子
			List<String> list = appDao.getRunAppidsByOtid(otid);
			for (int i = 0; i < list.size(); i++) {
				String appid = list.get(i);
				boolean isKilled = yarnUtil.killApp(appid);
				if (isKilled) {
					appDao.updateApplication("KILLED", appid);
				} else {
					return JavaUtil.createXml1( "error", JavaUtil.errMap("the application '" + appid
											+ "' is not normal to kill, Please check the operator task status"));
				}
			}
		}
		opTaskDao.updateOpTaskStatus("KILLED", otid); //更改数据库算子任务状态
		List<String> list1 = opTaskDao.getOptaskStatus(otid);
		String submittime = list1.get(1);
		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("status", "KILLED");
		mapsuc.put("submittime", submittime);
		return JavaUtil.createXml1("response", mapsuc); // 生成xml响应
	}

	/**
	 * 结束同步提交的算子任务，通过jobinstanceid和opid获得算子任务id，再调用killOpTaskAsync接口
	 * @param jobinstanceid 场景实例id
	 * @param opid 算子id
	 * @return
	 */
	@DELETE
	@Path("/{opid}/{jobinstanceid}")
	public String killOpTaskSync( @PathParam("jobinstanceid") String jobinstanceid,
			@PathParam("opid") String opid) throws Exception {
		String otid = opTaskDao.getOtid(jobinstanceid, opid);
		if ("nojobid".equals(otid)) {
			return JavaUtil.createXml1("errmsg", JavaUtil.errMap("the opTask(" + opid + ") of jobinstanceid( " + jobinstanceid + ")  is not submitted to the yarn"));
		} else {
			return killOpTaskAsync(otid);
		}
	}
	
	/**
	 * 获取执行Application的container日志 
	 * @param jobinstanceid 场景实例id
	 * @param opid 算子id
	 * @return
	 */
	@GET
	@Path("/{opid}/{jobinstanceid}/log")
	public String getApplicationLog(@PathParam("jobinstanceid") String jobinstanceid,
			@PathParam("opid") String opid) throws Exception {
		String otid = opTaskDao.getOtid(jobinstanceid, opid);
		if ("nojobid".equals(otid)) {
			return JavaUtil.createXml1("errmsg", JavaUtil.errMap("the opTask(" + opid + ") of jobinstanceid(" + jobinstanceid + ") is not exist"));
		}
		List<String> appids = appDao.getAllAppidsByOtid(otid);
		if (appids.size() == 0) {
			return JavaUtil.createXml1("errmsg", JavaUtil.errMap("当前算子标准输出流中没有截获到ApplicationId，" + "请确认是否用了log4j处理日志，应该使用标准输出"));
		}
		String localLogPath = InitConfig.LOCALAPPLOGS + otid + ".log." + JavaUtil.getTime().substring(11); 
		for (int i = 0; i < appids.size(); i++) { //遍历同一算子任务下的所有Application
			String appid = appids.get(i);
			yarnUtil.getContainersLogs(appid, localLogPath); //获得container日志，写到本地
		}
		String hdfsLogPath = InitConfig.HDFSAPPLOGS + otid + ".log." + JavaUtil.getTime().substring(11);
		hdfsUtil.putFileToHdfs(localLogPath, hdfsLogPath); //将写好的container日志上传到hdfs上
		
		Map<String, String> mapsuc = new HashMap<String, String>();
		mapsuc.put("logpath", "hdfs://" + hdfsLogPath);
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}

	/**
	 * 将hdfs的算子zip包下载到本地
	 * @param opid 算子id
	 * @param opjarPathHDFS 算子zip包的路径
	 */
	private void downloadOpFromHdfs(String opid, String opjarPathHDFS) throws Exception {	
		String opzipTemp = InitConfig.BASEPATH + "temp" + opjarPathHDFS.substring(opjarPathHDFS.lastIndexOf("/"));
		hdfsUtil.getFileFromHdfs(opjarPathHDFS, opzipTemp);	
		String opjarPathParent = opjarPathHDFS.substring(0, opjarPathHDFS.lastIndexOf("/") + 1);
		String opChecksumHdfsPath = opjarPathParent + InitConfig.CHECKSUM_DIR + opid + ".txt";
		String opChecksumLocalPath = InitConfig.BASEPATH + InitConfig.CHECKSUM_DIR + opid + ".txt";	
		hdfsUtil.getFileFromHdfs(opChecksumHdfsPath, opChecksumLocalPath);
	}

	/**
	 * 判断算子hdfs校验码与本地校验码是否一致
	 * @param opid 算子id
	 * @param opjarPathHDFS 算子zip包hdfs路径
	 * @return
	 */
	private boolean isEqualChecksum(String opid, String opjarPathHDFS) throws Exception {
		String localChecksumPath = InitConfig.BASEPATH + InitConfig.CHECKSUM_DIR + opid + ".txt";
		String localChecksum = JavaUtil.convertFileToString(localChecksumPath);
		String opjarParent = opjarPathHDFS.substring(0, opjarPathHDFS.lastIndexOf("/") + 1);
		String hdfsChecksumPath = opjarParent + InitConfig.CHECKSUM_DIR + opid + ".txt";	
		String hdfsChecksum = hdfsUtil.getStringFromHdfs(hdfsChecksumPath);
		if (!localChecksum.equals(hdfsChecksum)) {
			return false;
		}
		return true;
	}
	
	/**
	 * 将文件保存到HDFS
	 * @param xmlFile 原文件路径
	 * @param opid 算子id
	 */
	private String fileToHdfs(String xmlFile, String tempHdfsBasePath) throws Exception {
		if (!tempHdfsBasePath.endsWith("/")) {
			tempHdfsBasePath = tempHdfsBasePath + "/";
		}
		String inPath = tempHdfsBasePath + "stdin.xml";
		hdfsUtil.putFileToHdfs(xmlFile, inPath);
		return inPath;
	}

	/**
	 * 获得算子包里lib目录下的文件列表，设定LIBJARS环境变量
	 * @param opid 算子id
	 * @return
	 */
	private String getLIBJARS(String opid) {
		String libjarDir = InitConfig.BASEPATH + opid + "/lib/";
		String LIBJARS = "";
		File dirFile = new File(libjarDir);
		if (dirFile.exists() && dirFile.list().length > 0) {
			String[] files = dirFile.list();
			String tempLibjar = "";
			for (int i = 0; i < files.length; i++) {
				tempLibjar += "," + libjarDir + files[i];
			}
			LIBJARS = "LIBJARS=" + tempLibjar.substring(1);
		}
		return LIBJARS;
	}

	/**
	 * 获取InputStream保存成文件到本地和hdfs
	 * @param p 执行sun.sh的进程
	 * @param stdoutPath 日志信息输出路径
	 */
	private void getInputStream(final Process p, final String stdoutPath) {
		InputStream in = p.getInputStream();
		File file = new File(stdoutPath);
		File parentFile = file.getParentFile();
		if (!parentFile.exists()) {
			parentFile.mkdirs();
		}
		try {
			OutputStream ous = new FileOutputStream(file, true);
			byte[] buffer = new byte[1024];// 定义缓冲区
			int byteread = 0;
			while ((byteread = in.read(buffer)) != -1) {
				ous.write(buffer, 0, byteread);
			}
			ous.close();
			in.close();
		} catch (IOException e) {
			try {
				JavaUtil.writeExceptionToFile(e, stdoutPath);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * 获得hdfs上的算子输出信息
	 * @return 返回算子输出信息
	 */
	private String getStdoutFromHdfs(String hdfsbasepath) throws Exception {
		if (!hdfsbasepath.endsWith("/")) {
			hdfsbasepath = hdfsbasepath + "/";
		}
		String stdout = hdfsbasepath + "stdout.xml";
		String stderr = hdfsbasepath + "stderr.xml";
		String result = "";
		if (hdfsUtil.existFile(stdout)) {
			result = hdfsUtil.getStringFromHdfs(stdout);
		} else if (hdfsUtil.existFile(stderr)) {
			result = hdfsUtil.getStringFromHdfs(stderr);
		} else {
			Map<String, String> maperr = new HashMap<String, String>();
			maperr.put("errmsg", hdfsbasepath + "stdout.xml and " + hdfsbasepath + "stderr.xml not exist");
			return JavaUtil.createXml1("error", maperr); // 生成xml响应
		}
		return result;
	}

	/**
	 * 判断算子任务是否被kill
	 * @param otid 算子任务id
	 * @param stdoutPath 日志输出路径
	 * @return
	 */
	private boolean isOpTaskKilled(String otid, String stdoutPath) throws Exception {
		String otStatus = "";
		try {
			otStatus = opTaskDao.getOptaskStatus(otid).get(0);
		} catch (Exception e) {
			 JavaUtil.writeExceptionToFile(e, stdoutPath);
		}
		if ("KILLED".equals(otStatus)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 在数据库中记录算子任务信息
	 * @param otid 算子任务id
	 * @param opid 算子id
	 * @param queueName 队列名称
	 * @param userName 用户名称
	 * @param processId 场景id
	 * @param jobInstanceId 场景实例id
	 * @param submitTime 提交时间
	 * @param submission 提交方式
	 */
	private void recordOpTask(String otid, String opid, String queueName,
			String userName, String processId, String jobInstanceId,
			String submitTime, String submission) throws Exception {
		OpTask ot = new OpTask(otid, opid, processId, jobInstanceId, 
				userName, queueName,submitTime, submission, "RUNNING");
		opTaskDao.insertOpTask(ot); //插入数据库算子任务信息
	}

	/**
	 *     异步监测算子任务，获取ErrorStream,将ErrorStream保存成文件到本地，提取其中的applicationId，
	 * 并向数据库中插入application记录,每当出现一个新的Application，就查看上一个Application的状态，
	 * 并更新数据库，整个进程结束时查看最后一个Application的状态，更新数据库。
	 * 
	 * @param p 执行sun.sh的进程
	 * @param opid 算子id
	 * @param otid 算子任务id
	 * @param serialName 序列化名称
	 * @param stdoutPath 日志信息输入路径
	 */
	private void trackTaskAsync(final Process p, final String opid, // 异步
			final String otid, final String serialName, final String stdoutPath) {
		new Thread() {
			public void run() {
				InputStream in = p.getErrorStream();
				BufferedReader read = new BufferedReader(new InputStreamReader(in));
				String error = "";
				String tempError = "";
				try {
					File file = new File(InitConfig.STDERR + serialName.substring(0, 10) + "/" + opid + "_"
							+ serialName.substring(11) + ".xml");
					File parent = file.getParentFile();
					if (!parent.exists()) {
						parent.mkdirs();
					}
					FileWriter fwError = new FileWriter(file, true);
					String appid = "";
					int sequence = 0;
					FileWriter fwOut = new FileWriter(stdoutPath, true);
					while ((tempError = read.readLine()) != null) { //将信息写到日志文件
						error = tempError;
						fwOut.write(error + "\n");
						fwOut.flush();
						fwError.write(error + "\n");
						fwError.flush();
						String reg = "Submitted application";
						if (error.indexOf(reg) != -1) { //匹配Applicationid
							String regEx = "application_[0-9]{13}_[0-9]{4}$";
							Pattern pat = Pattern.compile(regEx);
							Matcher mat = pat.matcher(error);
							if (mat.find()) {
								if (sequence != 0) {
									String status = yarnUtil.getAppStatus(appid);
									appDao.updateApplication(status, appid);
								}
								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
								String startTime = sdf.format(new Date()).toString();
								appid = mat.group(0);
								sequence = sequence + 1;
								ApplicationInstance app = new ApplicationInstance(appid, otid, sequence, "RUNNING", startTime);
								appDao.insertApplication(app); //插入新的Application记录
							}
						}
					}
					fwOut.close();
					fwError.close();
					in.close();
					read.close();
					if (!"".equals(appid)) {
					    String status = yarnUtil.getAppStatus(appid);
					    appDao.updateApplication(status, appid); //更新Application状态
					    opTaskDao.updateOpTaskStatus(status, otid); //更新算子任务状态
					} else {
						opTaskDao.updateOpTaskStatus("FINISHED", otid); //针对于hive算子id为MR.*.*的情况
					}	
					
					getInputStream(p, stdoutPath);//监测inputStream的信息
				} catch (Exception e) {
					try {
						JavaUtil.writeExceptionToFile(e, stdoutPath);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
		}.start();
	}

	/**
	 *     异步监测hive算子任务，获取ErrorStream,将ErrorStream保存成文件到本地，
	 * 当进程执行结束后，查看hive算子任务状态，并将状态更新到数据库。
	 * @param p 执行算子的进行
	 * @param opid 算子id
	 * @param otid 算子任务id
	 * @param serialName 序列化名称
	 * @param stdoutPath 日志输出路径
	 */
	private void trackTaskAsync_hive(final Process p, final String opid, // 异步
			final String otid, final String serialName, final String stdoutPath) {
		new Thread() {
			public void run() {
				InputStream in = p.getErrorStream();
				BufferedReader read = new BufferedReader(new InputStreamReader(in));
				String error= "";
				String tempError = "";
				try {
					File file = new File(InitConfig.STDERR + serialName.substring(0, 10) + "/" + opid + "_"
							+ serialName.substring(11) + ".xml");
					File parent = file.getParentFile();
					if (!parent.exists()) {
						parent.mkdirs();
					}
					FileWriter fwError = new FileWriter(file, true);
					FileWriter fwOut = new FileWriter(stdoutPath, true);
					while ((tempError = read.readLine()) != null) {
						error = tempError;
						fwOut.write(error + "\n");
						fwOut.flush();
						fwError.write(error + "\n");
						fwError.flush();
					}
					fwOut.close();
					fwError.close();
					in.close();
					read.close();
					
					String opTaskStatus = "";
					HiveInstanceDAO hiDao = new HiveInstanceDAO();
					if (!hiDao.existHiveInstByOtid(otid)) { 
						JavaUtil.log("业务数据库HiveInstance中没有该otid（" + otid + "）的记录", stdoutPath);
						opTaskStatus = "FAILED";
					} else {
						HiveUtil hu = new HiveUtil(otid);
						String opState = hu.getStatus();
						if ("INITIALIZED_STATE".equals(opState) || "RUNNING_STATE".equals(opState) || "PENDING_STATE".equals(opState)) {
							opTaskStatus = "RUNNING";
						} else if ("FINISHED_STATE".equals(opState)) {
							opTaskStatus = "SUCCEEDED";
						} else if ("CANCELED_STATE".equals(opState)) {
							opTaskStatus = "KILLED";
						} else {
							opTaskStatus = "FAILED";
						}
						hu.closeOperationReq();
						hiDao.updateHiveInstanceStatus(otid, opState);
					}		
					opTaskDao.updateOpTaskStatus(opTaskStatus, otid);
					
					getInputStream(p, stdoutPath);//监测inputStream的信息
				} catch (Exception e) {
					try {
						JavaUtil.writeExceptionToFile(e, stdoutPath);
					} catch (Exception e1) {
						e1.printStackTrace();
					}
				}
			}
		}.start();
	}

	/**
	 * 同步监测算子任务，流程与异步相似
	 * @param p 执行算子任务的进程
	 * @param opid 算子id
	 * @param otid 算子任务id
	 * @param serialName 序列化名称
	 * @param stdoutPath 日志输出路径
	 */
	private void trackTaskSync(final Process p, final String opid, // 异步
			final String otid, final String serialName, final String stdoutPath) {
		InputStream in = p.getErrorStream();
		BufferedReader read = new BufferedReader(new InputStreamReader(in));
		String error = "";
		String tempErro = "";
		try {
			File file = new File(InitConfig.STDERR + serialName.substring(0, 10) + "/" + opid + "_"
					+ serialName.substring(11) + ".xml");
			File parent = file.getParentFile();
			if (!parent.exists()) {
				parent.mkdirs();
			}
			FileWriter fwError = new FileWriter(file, true);
			String appid = "";
			int sequence = 0;
			FileWriter fwOut = new FileWriter(stdoutPath, true);
			while ((tempErro = read.readLine()) != null) {
				error = tempErro;
				fwOut.write(error + "\n");
				fwOut.flush();
				fwError.write(error + "\n");
				fwError.flush();
				String reg = "Submitted application";
				if (error.indexOf(reg) != -1) { 
					String regEx = "application_[0-9]{13}_[0-9]{4}$";
					Pattern pat = Pattern.compile(regEx);
					Matcher mat = pat.matcher(error);
					if (mat.find()) {
						if (sequence != 0) {
							String status = yarnUtil.getAppStatus(appid);
							appDao.updateApplication(status, appid);
						}
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
						String startTime = sdf.format(new Date()).toString();
						appid = mat.group(0);
						sequence = sequence + 1;
						ApplicationInstance app = new ApplicationInstance(appid, otid, sequence, "RUNNING", startTime);
						appDao.insertApplication(app);
					}
				}
			}
			fwOut.close();
			fwError.close();
			in.close();
			read.close();
			
			//当算子中使用log4j时，监测inputStream信息
			if ("".equals(appid)) {
				InputStream ins = p.getInputStream();
				BufferedReader bufRead = new BufferedReader(new InputStreamReader(ins));
				String input = "";
				String tempInput = "";
				while ((tempInput = bufRead.readLine()) != null) {
					input = tempInput;
					String reg = "Submitted application";
					if (input.indexOf(reg) != -1) { 
						String regEx = "application_[0-9]{13}_[0-9]{4}$";
						Pattern pat = Pattern.compile(regEx);
						Matcher mat = pat.matcher(input);
						if (mat.find()) {
							if (sequence != 0) {
								String status = yarnUtil.getAppStatus(appid);
								appDao.updateApplication(status, appid);
							}
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
							String startTime = sdf.format(new Date()).toString();
							appid = mat.group(0);
							sequence = sequence + 1;
							ApplicationInstance app = new ApplicationInstance(appid, otid, sequence, "RUNNING", startTime);
							appDao.insertApplication(app);
						}
					}
				}
			}	
			
			if (!"".equals(appid)) {
			    String status = yarnUtil.getAppStatus(appid);
			    appDao.updateApplication(status, appid);
			    opTaskDao.updateOpTaskStatus(status, otid);
			} else {
				opTaskDao.updateOpTaskStatus("FINISHED", otid); //针对于hive算子id为MR.*.*的情况
			}	
		} catch (Exception e) {
			try {
				JavaUtil.writeExceptionToFile(e, stdoutPath);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

	}

	/**
	 * 同步监测hive算子任务，流程与异步相似
	 * @param p 执行算子的进行
	 * @param opid 算子id
	 * @param otid 算子任务id
	 * @param serialName 序列化名称
	 * @param stdoutPath 日志输出路径
	 */
	private void trackTaskSync_hive(final Process p, final String opid, 
			final String otid, final String serialName, final String stdoutPath) {
		InputStream in = p.getErrorStream();
		BufferedReader read = new BufferedReader(new InputStreamReader(in));
		String erro1 = "";
		String tempErro1 = "";
		try {
			File file = new File(InitConfig.STDERR + serialName.substring(0, 10) + "/" + opid + "_"
					+ serialName.substring(11) + ".xml");
			File parent = file.getParentFile();
			if (!parent.exists()) {
				parent.mkdirs();
			}
			FileWriter fwError = new FileWriter(file, true);
			FileWriter fwOut = new FileWriter(stdoutPath, true);
			while ((tempErro1 = read.readLine()) != null) {
				erro1 = tempErro1;
				fwOut.write(erro1 + "\n");
				fwOut.flush();
				fwError.write(erro1 + "\n");
				fwError.flush();
			}
			fwOut.close();
			fwError.close();
			in.close();
			read.close();
			
			String opTaskStatus = "";
			HiveInstanceDAO hiDao = new HiveInstanceDAO();
			if (!hiDao.existHiveInstByOtid(otid)) { 
				JavaUtil.log("业务数据库HiveInstance中没有该otid（" + otid + "）的记录", stdoutPath);
				opTaskStatus = "FAILED";
			} else {
				HiveUtil hu = new HiveUtil(otid);
				String opState = hu.getStatus();
				if ("INITIALIZED_STATE".equals(opState) || "RUNNING_STATE".equals(opState) || "PENDING_STATE".equals(opState)) {
					opTaskStatus = "RUNNING";
				} else if ("FINISHED_STATE".equals(opState)) {
					opTaskStatus = "SUCCEEDED";
				} else if ("CANCELED_STATE".equals(opState)) {
					opTaskStatus = "KILLED";
				} else {
					opTaskStatus = "FAILED";
				}
				hu.closeOperationReq();
				hiDao.updateHiveInstanceStatus(otid, opState);
			}			
			opTaskDao.updateOpTaskStatus(opTaskStatus, otid);	
		} catch (Exception e) {
			try {
				JavaUtil.writeExceptionToFile(e, stdoutPath);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * 同步监测不依赖注册提交的算子任务，获取ErrorStream,提取其中的applicationId，将ErrorStream保存成文件到本地
	 * @param p 执行算子的进程
	 * @param opid 算子id
	 * @param otid 算子任务id
	 */
	private void trackTaskNoRegister(final Process p, final String opid,
			final String serialName, final String stdoutPath) {
		String applicationId = "";
		InputStream in = p.getErrorStream();
		BufferedReader read = new BufferedReader(new InputStreamReader(in));
		String error = "";
		String tempError = "";
		File fileErr = new File(InitConfig.STDERR + serialName.substring(0, 10)
				+ "/" + opid + "_" + serialName.substring(11) + ".txt");
		File parentErr = fileErr.getParentFile();
		if (!parentErr.exists()) {
			parentErr.mkdirs();
		}
		try {
			FileWriter fwOut = new FileWriter(stdoutPath, true);
			FileWriter fwError = new FileWriter(fileErr, true);
			while ((tempError = read.readLine()) != null) {
				error = tempError;
				fwOut.write(error + "\n");
				fwOut.flush();
				fwError.write(error + "\n");
				fwError.flush();
				String reg = "Submitted application";
				if (error.indexOf(reg) != -1) {
					String regEx = "application_[0-9]{13}_[0-9]{4}$";
					Pattern pat = Pattern.compile(regEx);
					Matcher mat = pat.matcher(error);
					if (mat.find()) {
						applicationId = mat.group(0);
						File fileAppid = new File(InitConfig.APPID + serialName.substring(0, 10) + "/" + opid
								+ "_" + serialName.substring(11) + ".txt");
						File parentappid = fileAppid.getParentFile();
						if (!parentappid.exists()) {
							parentappid.mkdirs();
						}
						FileWriter fw = new FileWriter(fileAppid, true);
						fw.write("applicationId:" + applicationId + "\n");
						fw.flush();
						fw.close();
					}
				}
			}
			fwOut.close();
			fwError.close();
			in.close();
			read.close();
		} catch (IOException e) {
			try {
				JavaUtil.writeExceptionToFile(e, stdoutPath);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	/**
	 * 解析请求文件
	 * @param xmlFilePath 请求文件保存路径
	 * @return
	 */
	private Map<String, String> parseRequest(String xmlFilePath) throws Exception {
		boolean existJobinstanceid = false;
		boolean existQueueName = false;
		boolean existUserName = false;
		boolean existProcessId = false;
		boolean existtempHdfsBasePath = false;
		Map<String, String> map = new HashMap<String, String>();
		map.put("error", "");
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		
		Element rootElement = document.getRootElement(); // 获得根节点
		if (rootElement.selectSingleNode("jobinstanceid") != null) {
			existJobinstanceid = true;
			String jobinstanceid = rootElement.selectSingleNode("jobinstanceid").getText();
			if(jobinstanceid.length() > 128) {
				map.put("error", "jobInstanceId长度不能大于128");
			}
			map.put("PIID", "PIID=" + jobinstanceid);
		}
		if (rootElement.selectSingleNode("context") == null) {
			map.put("error", "the context is null");
			return map;
		}
		Element contextElement = rootElement.element("context");
		Iterator<?> iter = contextElement.elementIterator();
		while (iter.hasNext()) {
			Element propElement = (Element) iter.next();
			if ("queueName".equals(propElement.attributeValue("name"))) {
				existQueueName = true;
				String queueName = propElement.attributeValue("value");
				if(queueName.length() > 128) {
					map.put("error", "queueName长度不能大于128");
				}
				map.put("QUEUE", "QUEUE=" + queueName);
			}
			if ("userName".equals(propElement.attributeValue("name"))) {
				existUserName = true;
				String userName = propElement.attributeValue("value");
				if(userName.length() > 128) {
					map.put("error", "userName长度不能大于128");
				}
				if ("".equals(userName)) {
					map.put("error", "the property userName is null");
				}
				map.put("USER", "USER=" + userName);
			}
			if ("processId".equals(propElement.attributeValue("name"))) {
				existProcessId = true;
				String processId = propElement.attributeValue("value");
				if(processId.length() > 128) {
					map.put("error", "processId长度不能大于128");
				}
				if ("".equals(processId)) {
					map.put("error", "the property processId is null");
				}
				map.put("PROCID", "PROCID=" + processId);
			}
			if ("tempHdfsBasePath".equals(propElement.attributeValue("name"))) {
				existtempHdfsBasePath = true;
				if ("".equals(propElement.attributeValue("value"))) {
					map.put("error", "the property tempHdfsBasePath is null");
				}
				map.put("tempHdfsBasePath", propElement.attributeValue("value"));
			}
		}
		if (existJobinstanceid != true) {
			map.put("error", "the Jobinstanceid is null");
		}
		if (existQueueName != true) {
			map.put("QUEUE", "QUEUE=default");
		}
		if (existUserName != true) {
			map.put("error", "the UserName is null");
		}
		if (existProcessId != true) {
			map.put("error", "the ProcessId is null");
		}
		if (existtempHdfsBasePath != true) {
			map.put("error", "the tempHdfsBasePath is null");
		}
		return map;	
	}

	/**
	 * 设置环境变量，因为算子是在用户自定义的环境下执行的，需要把原系统变量设置进来，再添加自定义的环境变量
	 * @param otid 算子任务id
	 * @param CONF 环境变量：算子输入文件路径
	 * @param OPJAR 环境变量：要执行的算子jar包
	 * @param LIBJARS 环境变量：算子第三方依赖
	 * @param QUEUE 环境变量：队列
	 * @param USER 环境变量： 提交用户
	 * @param opid 算子id
	 * @param PROCID 环境变量：场景id
	 * @param PIID 环境变量：场景实例id
	 * @param WORKDIR 环境变量：执行算子的工作目录
	 * @param stdoutPath 日志输出路径
	 * @return
	 */
	private String[] setEnvp(String otid, String CONF, String OPJAR, String LIBJARS, String QUEUE, String USER, String opid,
			String PROCID, String PIID, String WORKDIR, String stdoutPath) throws Exception {
		Map<String, String> map = System.getenv();
		Set<String> keys = map.keySet();
		Iterator<String> it = keys.iterator();
		List<String> list = new ArrayList<String>();
		while (it.hasNext()) { //设置原系统变量
			String key = it.next();
			String value = (String) map.get(key);
			String ev = key + "=" + value;
			list.add(ev);
		}
		JavaUtil.log("system env>>>>>>>>>>>>", stdoutPath);
		JavaUtil.log(list.toString(), stdoutPath);
		//添加自定义环境变量
		list.add("otid=" + otid);
		list.add(CONF);
		list.add(OPJAR);
		list.add(LIBJARS);
		list.add(QUEUE);
		list.add(USER);
		list.add("OPID=" + opid);
		list.add(PROCID);
		list.add(PIID);
		list.add(WORKDIR);
		String[] envp = new String[list.size()];
		list.toArray(envp);
		return envp;
	}

	/**
	 * 解压下载到本地的算子zip包
	 * @param opid 算子id
	 * @param stdoutPath 日志输出信息
	 */
	private void unzipTemp(String opid, String stdoutPath) throws Exception {
		OperatorDAO opdao = new OperatorDAO();
		String opjarPath = opdao.getOpjarPath(opid);
		String opzipTemp = InitConfig.BASEPATH + "temp" + opjarPath.substring(opjarPath.lastIndexOf("/"));
		File opzipTempFile = new File(opzipTemp);
		JavaUtil.unzip(opzipTempFile, InitConfig.BASEPATH, stdoutPath);
	}

}
