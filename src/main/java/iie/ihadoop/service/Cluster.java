package iie.ihadoop.service;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.json.JSONArray;
import org.json.JSONObject;

import iie.ihadoop.util.CMUtil;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;
import iie.ihadoop.util.YarnUtil;

@Path("/queues")
public class Cluster {
	CMUtil cmUtil = new CMUtil();
	YarnUtil yarnUtil = new YarnUtil();
	
	/**
	 * 创建队列
	 * @param request 请求
	 * @return 返回创建队列的信息
	 * @throws Exception 抛出异常
	 */
	@POST
	public String createQueue(@Context HttpServletRequest request) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		InputStream in = request.getInputStream();
		String requestFilePath = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "createQueue/"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> resultParseRequest = parseCreateRequest(requestFilePath); // 解析xml请求参数
		if (!"".equals(resultParseRequest.get("error"))) {
			return JavaUtil.createXml1("error", JavaUtil.errMap(resultParseRequest.get("error")));
		}
		
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");
		if(yarnUtil.existQueue(schedulerInfo, resultParseRequest.get("queueName"))) { //存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("exist queue: " + resultParseRequest.get("queueName")));
		}

		double totalMemory = cmUtil.calculateTotalMemory(); //得到nodemanager内存总大小	
		double requestMem =  Double.parseDouble(resultParseRequest.get("memtotal"));
		double userLimitFactor = Double.parseDouble(resultParseRequest.get("userlimitfactor"));
		double newQueueCapacity = Math.round(100 * (requestMem / totalMemory));

		String url = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName() 
				+ "/roleConfigGroups/" + cmUtil.getRMConfigGroupName() + "/config";
		String configGroup = cmUtil.getMethod(url); //获得cms的角色组配置
		String queueConf = getQueueConfig(configGroup); //取得角色组配置中的队列配置
		String queueName = resultParseRequest.get("queueName");
		queueConf = updateQueueConf_createQueue(queueConf, queueName, newQueueCapacity, userLimitFactor); //修改队列配置
		if ("failed".equals(queueConf)) { //如果请求资源不符合要求，返回错误信息
			return  JavaUtil.createXml1("error", JavaUtil.errMap("The cluster does not have enough memory resource"));
		}
		String updateConfig = updateConfig(configGroup,queueConf); //获得配置的修改内容
		cmUtil.putMethod(url, updateConfig); //向cms发送put请求修改配置
		String response = cmUtil.refreshQueue_CMS(); //刷新队列
		if ("UNKNOWN".equals(response)) {
			return  JavaUtil.createXml1("error", JavaUtil.errMap("ResourceManager的状态是UNKNOWN,有可能是YARN在重启"));
		}

		for (int i = 0; i < 20; i++) {
			String queueStatus = getQueueSatusTemp(queueName);
			System.out.println("发送refreshQueue请求后的时间>>>>>>>>>>>" + i*5 + "s");
			if (!"no".equals(getQueueSatusTemp(queueName))) {
				return queueStatus;
			}	
			Thread.sleep(5000);
		}
		return  JavaUtil.createXml1("error", JavaUtil.errMap("请调用查看队列详情接口查看该队列是否创建成功，若不成功请重新创建")); 
	}
	
	/**
	 * 删除队列
	 * @param queueName 删除的队列名称
	 * @return 返回空的errmsg
	 * @throws Exception
	 */
	@DELETE
	@Path("/delete/{queueName}")
	public String deleteQueue(@PathParam("queueName") String queueName) throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");//获得调度器信息
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("not exist queue: " + queueName));
		}
		
		String url = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName()
				+ "/roleConfigGroups/" + cmUtil.getRMConfigGroupName() + "/config";
		String configGroup = cmUtil.getMethod(url);
		String queueConf = getQueueConfig(configGroup); 
		queueConf = updateQueueConf_deleteQueue(queueConf, queueName);
	    String updateConfig = updateConfig(configGroup,queueConf);
	    
	    cmUtil.putMethod(url, updateConfig);
		String urlRestart = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName() + "/commands/restart";
		cmUtil.postMethod(urlRestart);
		return JavaUtil.createXml1("response", JavaUtil.errMap(""));
	}
	
	/**
	 * 查看集群所有队列信息
	 * @return 返回所有队列信息
	 * @throws Exception 抛出异常
	 */
	@GET
	public String getQueuesStatus() throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");
		Element queuedescs = schedulerinfoToQueuedescs(schedulerInfo); //通过调度器信息组织队列描述信息
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		Element errmsgElement = rootElement.addElement("errmsg");
		errmsgElement.setText("");
		rootElement.add(queuedescs);
		
		return JavaUtil.formatXML(document);
	}
	
	/**
	 * 查看某个队列信息
	 * @param queueName 队列名称
	 * @return 返回该队列信息
	 * @throws Exception 抛出异常
	 */
	@GET
	@Path("/{queueName}")
	public String getQueueSatus(@PathParam("queueName") String queueName) throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("not exist queue: " + queueName));
		}
		
		Element queuedesc = schedulerinfoToQueuedesc( schedulerInfo, queueName); //通过调度器信息和队列名称组织队列描述信息
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		Element errmsgElement = rootElement.addElement("errmsg");
		errmsgElement.setText("");
		rootElement.add(queuedesc);
		return JavaUtil.formatXML(document);
	}

    /**
     * 查看集群资源接口
     * @return 返回集群总内存、cpu信息
     * @throws Exception 抛出异常
     */
	@GET
	@Path("/calcresource")
	public String getCalcresource() throws Exception {
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		Element errorElement = rootElement.addElement("errmsg");
		Element calcresourceElement = rootElement.addElement("calcresource");
		Element memtotalElement = calcresourceElement.addElement("memtotal");
		Element cputotalElement = calcresourceElement.addElement("cputotal");
		errorElement.setText("");
		String memtotal = String.valueOf(cmUtil.calculateTotalMemory());
		memtotalElement.setText(memtotal + "MB");
		String cputotal = String.valueOf(cmUtil.calculateTotalCpu());
		cputotalElement.setText(cputotal);
		return JavaUtil.formatXML(document);
	}
	
	/**
	 * 更新队列
	 * @param request 请求
	 * @param queueName 队列名称
	 * @return 返回修改后队列信息
	 * @throws Exception 抛出异常
	 */
	@PUT
	@Path("/{name}")
	public String putQueue(@Context HttpServletRequest request,
			@PathParam("name") String queueName) throws Exception {
		String serialName = JavaUtil.getTime(); // 获取时间戳
		InputStream in = request.getInputStream();
		String requestFilePath1 = InitConfig.REQUESTFILEPATH_OTHER + serialName.substring(0, 10) + "/" + "putQueue/"
				+ serialName.substring(11) + ".xml";
		JavaUtil.writeInputStreamToFile(in, requestFilePath1); // 将接收的xml配置信息保存到文件
		in.close();
		Map<String, String> resultParseRequest = parseCreateRequest(requestFilePath1); // 解析xml请求参数
		if (!"".equals(resultParseRequest.get("error"))) {
			Map<String, String> maperr = new HashMap<String, String>();
			maperr.put("errmsg", resultParseRequest.get("error"));
			return JavaUtil.createXml1("error", maperr); // 生成xml响应
		}

		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("not exist queue: " + queueName));
		}
		
		double totalMemory = cmUtil.calculateTotalMemory(); //得到nodemanager内存总大小	
		double requestMem =  Double.parseDouble(resultParseRequest.get("memtotal"));
		double newQueueCapacity =  Math.round(100 * (requestMem / totalMemory));
		double userLimitFactor = Double.parseDouble(resultParseRequest.get("userlimitfactor"));
		String url = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName() 
				+ "/roleConfigGroups/" + cmUtil.getRMConfigGroupName() + "/config";
		String configGroup = cmUtil.getMethod(url);
		String queueConf = getQueueConfig(configGroup); //
		
		List<Object> list = updateQueueConf_upateQueue(queueConf, queueName, newQueueCapacity, userLimitFactor);
		queueConf = (String) list.get(0);
		if ("failed".equals(queueConf)) { //如果请求资源不符合要求，返回错误信息
			return  JavaUtil.createXml1("error", JavaUtil.errMap("The cluster does not have enough memory resource"));
		}
		String updateConfig = updateConfig(configGroup,queueConf);
		
		cmUtil.putMethod(url, updateConfig);
		String response = cmUtil.refreshQueue_CMS(); //刷新队列
		if ("UNKNOWN".equals(response)) {
			return  JavaUtil.createXml1("error", JavaUtil.errMap("ResourceManager的状态是UNKNOWN,有可能是YARN在重启"));
		}

		if (list.size() > 1) {
			String changeName = (String) list.get(1);
			double changeValue = (double) list.get(2);
			for (int i = 0; i < 40; i++) {
				System.out.println("putQueue接口发送refreshQueue请求后的时间>>>>>>>>>>>" + i*5 + "s");
				if (yarnUtil.isQueueChanged(queueName, changeName, changeValue))
					return getQueueSatusTemp(queueName);
				Thread.sleep(5000);
			}
			return  JavaUtil.createXml1("error", JavaUtil.errMap("请调用查看队列详情接口查看该队列是否修改成功，若不成功请重新修改"));
		}
		return getQueueSatusTemp(queueName); 
	}

	
	/**
	 * 停止使用队列，再将任务提交到stop的队列，任务将直接失败
	 * @param queueName 队列名称
	 * @return 返回空的errmsg
	 * @throws Exception 抛出异常
	 */
	@DELETE
	@Path("/stop/{queueName}")
	public String stopQueue(@PathParam("queueName") String queueName) throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");//获得调度器信息
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("not exist queue: " + queueName));
		}
		String url = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName() 
				+ "/roleConfigGroups/" + cmUtil.getRMConfigGroupName() + "/config";
		String configGroup = cmUtil.getMethod(url);
		String queueConf = getQueueConfig(configGroup); 
		queueConf = updateQueueConf_stop_start(queueConf, queueName, "STOPPED");
	    String updateConfig = updateConfig(configGroup,queueConf);
		
	    cmUtil.putMethod(url, updateConfig);
	    String response = cmUtil.refreshQueue_CMS(); //刷新队列
		if ("UNKNOWN".equals(response)) {
			return  JavaUtil.createXml1("error", JavaUtil.errMap("ResourceManager的状态是UNKNOWN,有可能是YARN在重启"));
		}
		
		Map<String, String> mapsuc = new HashMap<String, String>(); 
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}
	
	/**
	 * 启动队列，与stopQueue接口向对应，队列就能正常运行
	 * @param queueName 队列名称
	 * @return 返回空的errmsg
 	 * @throws Exception 抛出异常
	 */
	@POST
	@Path("/start/{queueName}")
	public String startQueue(@PathParam("queueName") String queueName) throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = cmUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");//获得调度器信息
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return JavaUtil.createXml1("error", JavaUtil.errMap("not exist queue: " + queueName));
		}
		String url = InitConfig.CM_ADDR + "/api/v9/clusters/" + cmUtil.getClusterName() + "/services/" + cmUtil.getYarnServiceName() 
				+ "/roleConfigGroups/" + cmUtil.getRMConfigGroupName() + "/config";
		String configGroup = cmUtil.getMethod(url);
		String queueConf = getQueueConfig(configGroup); 
		queueConf = updateQueueConf_stop_start(queueConf, queueName, "RUNNING");
	    String updateConfig = updateConfig(configGroup,queueConf);
		
	    cmUtil.putMethod(url, updateConfig);
	    String response = cmUtil.refreshQueue_CMS(); //刷新队列
		if ("UNKNOWN".equals(response)) {
			return  JavaUtil.createXml1("error", JavaUtil.errMap("ResourceManager的状态是UNKNOWN,有可能是YARN在重启"));
		}
		
		Map<String, String> mapsuc = new HashMap<String, String>(); 
		mapsuc.put("errmsg", "");
		return JavaUtil.createXml1("response", mapsuc);
	}
	
	/**
	 * 解析创建存储单元请求文件
	 * 
	 * @param xmlFilePath 请求文件本地保存路径
	 */
	private Map<String, String> parseCreateRequest(String xmlFilePath) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		map.put("error", "");
		boolean existQueueName = false;
		boolean existMemtotal = false;
		String userlimitfactor = "1.0";
		File file = new File(xmlFilePath);
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(file);
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator(); // 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if ("name".equals(node2.getName())) {
				existQueueName = true;
				String queueName = node2.getText();
				map.put("queueName", queueName);
			}
			if ("calcresource".equals(node2.getName())) {
				Iterator<?> iter_calcr = node2.elementIterator();
				while (iter_calcr.hasNext()) {
					Element node_calcr = (Element) iter_calcr.next();
					if ("memtotal".equals(node_calcr.getName())) {
						existMemtotal = true;
						String memtotal = node_calcr.getText();
						map.put("memtotal", memtotal);
					}
					if ("userlimitfactor".equals(node_calcr.getName())) {
						userlimitfactor = node_calcr.getText();
					}
				}
			}
		}
		map.put("userlimitfactor", userlimitfactor);
		if (!existQueueName) {
			map.put("error", "queueName is not exist");
		}
		if (!existMemtotal) {
			map.put("error", "memtotal is not exist");
		}
		return map;
	}

	/**
	 * 从角色组的配置信息中获得队列的具体信息
	 * @param config 角色组的配置信息
	 * @return 返回队列配置信息
	 * @throws Exception
	 */
	private String getQueueConfig(String config) throws Exception {
		String queuetmp = "";
		JSONObject jsonobject = new JSONObject(config);
		JSONArray jsonarray = jsonobject.getJSONArray("items");
		for(int i = 0; i < jsonarray.length(); i++) {
			JSONObject jsonobject1 = jsonarray.getJSONObject(i);
			if ("resourcemanager_capacity_scheduler_configuration".equals(jsonobject1.getString("name"))) {
				queuetmp = jsonobject1.getString("value");
			}
		}
		return queuetmp;
	}
	
	/**
	 * 获得队列信息，服务于创建队列接口和修改队列接口
	 * @param queueName 队列名称
	 * @return 返回队列信息
	 * @throws Exception
	 */
	private String getQueueSatusTemp(String queueName) throws Exception {
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		String schedulerInfo = JavaUtil.getMethod("http://" + rmWebAppAddr + "/ws/v1/cluster/scheduler");
		if(!yarnUtil.existQueue(schedulerInfo, queueName)) { //不存在该队列，返回错误信息
			return "no";
		}
		Element queuedesc = schedulerinfoToQueuedesc( schedulerInfo, queueName); //通过调度器信息和队列名称组织队列描述信息
		Document document = DocumentHelper.createDocument();
		Element rootElement = document.addElement("response");
		Element errmsgElement = rootElement.addElement("errmsg");
		errmsgElement.setText("");
		rootElement.add(queuedesc);
		return JavaUtil.formatXML(document);
	}
	
    /**
     * 将调度器信息改为某个队列描述信息
     * @param schedulerinfo 调度器信息
     * @param queueName 队列名称
     * @return 返回队列描述信息
     * @throws Exception
     */
	private Element schedulerinfoToQueuedesc (String schedulerinfo, String queueName) throws Exception {
		Document queuedescsDoc = DocumentHelper.createDocument();
		Element queuedescElement = queuedescsDoc.addElement("queuedesc");
		JSONObject schedulerinfo_json = new JSONObject(schedulerinfo);
		JSONArray queues_json = schedulerinfo_json.getJSONObject("scheduler").getJSONObject("schedulerInfo").getJSONObject("queues").getJSONArray("queue");
		for (int i = 0; i < queues_json.length(); i++) {	
			JSONObject queue_json = queues_json.getJSONObject(i);
			if (!queueName.equals(queue_json.getString("queueName"))) {
				continue;
			}
			Element nameElement = queuedescElement.addElement("name");
			Element maxappsElement = queuedescElement.addElement("maxapps");
			Element numactiveappsElement = queuedescElement.addElement("numactiveapps");
			Element numpendingappsElement = queuedescElement.addElement("numpendingapps");
			Element numfinishedappsElement = queuedescElement.addElement("numfinishedapps");
			Element typeElement = queuedescElement.addElement("type");
			Element userLimitFactorElement = queuedescElement.addElement("userLimitFactor");
			Element calcresourceElement = queuedescElement.addElement("calcresource");
			Element memtotalElement = calcresourceElement.addElement("memtotal");
			Element memusedElement = calcresourceElement.addElement("memused");
			Element memavailElement = calcresourceElement.addElement("memavail");
			Element cputotalElement = calcresourceElement.addElement("cputotal");
			Element cpuusedElement = calcresourceElement.addElement("cpuused");
			Element cpuavailElement = calcresourceElement.addElement("cpuavail");		
			
			String maxApplications = queue_json.getString("maxApplications");
			String numActiveApplications = queue_json.getString("numActiveApplications");
			String numPendingApplications = queue_json.getString("numPendingApplications");
			double capacity = Double.parseDouble(queue_json.getString("capacity"));
			double maxCapacity = Double.parseDouble(queue_json.getString("maxCapacity")); 
			double userLimitFactor = Double.parseDouble(queue_json.getString("userLimitFactor"));
			double memused =  queue_json.getJSONObject("resourcesUsed").getDouble("memory");
			double cpuused =  queue_json.getJSONObject("resourcesUsed").getDouble("vCores");
			String type = "capacity";
			double userLimitCapacity = capacity * userLimitFactor;
			double memtotal =0.0;
			if (maxCapacity < userLimitCapacity) {
				memtotal = cmUtil.calculateTotalMemory();
			} else {
			    memtotal = (cmUtil.calculateTotalMemory() * capacity * userLimitFactor) / 100;
			}
			memtotal = Math.ceil(memtotal);
			double memavail = memtotal - memused;
			double cputotal = (cmUtil.calculateTotalCpu() * capacity * userLimitFactor) / 100;
			cputotal = Math.ceil(cputotal);
			double cpuavail = cputotal - cpuused;
			String numfinishedapps = String.valueOf(yarnUtil.getNumFinishedApps(queueName));
			
			nameElement.setText(queueName);
			maxappsElement.setText(maxApplications);
			numactiveappsElement.setText(numActiveApplications);
			numpendingappsElement.setText(numPendingApplications);	
			numfinishedappsElement.setText(numfinishedapps);
			typeElement.setText(type);
			userLimitFactorElement.setText(String.valueOf(userLimitFactor));
			memtotalElement.setText(String.valueOf(memtotal));
			memusedElement.setText(String.valueOf(memused));
			memavailElement.setText(String.valueOf(memavail));
			cputotalElement.setText(String.valueOf(cputotal));
			cpuusedElement.setText(String.valueOf(cpuused));
			cpuavailElement.setText(String.valueOf(cpuavail));
			break;
		}
		return queuedescElement;
	}
	
	/**
	 * 将调度器信息改为所有队列描述信息
	 * @param schedulerinfo 调度器信息
	 * @return 所有队列描述信息
	 * @throws Exception
	 */
	private Element schedulerinfoToQueuedescs(String schedulerinfo) throws Exception {
		Document queuedescsDoc = DocumentHelper.createDocument();
		Element queuedescsElement = queuedescsDoc.addElement("queuesescs");		
		JSONObject schedulerinfo_json = new JSONObject(schedulerinfo);
		JSONArray queues_json = schedulerinfo_json.getJSONObject("scheduler").getJSONObject("schedulerInfo").getJSONObject("queues").getJSONArray("queue");
		for (int i = 0; i < queues_json.length(); i++) {
			Element queuedescElement = queuedescsElement.addElement("queuedesc");
			Element nameElement = queuedescElement.addElement("name");
			Element maxappsElement = queuedescElement.addElement("maxapps");
			Element numactiveappsElement = queuedescElement.addElement("numactiveapps");
			Element numpendingappsElement = queuedescElement.addElement("numpendingapps");
			Element numfinishedappsElement = queuedescElement.addElement("numfinishedapps");
			Element typeElement = queuedescElement.addElement("type");
			Element userLimitFactorElement = queuedescElement.addElement("userLimitFactor");
			Element calcresourceElement = queuedescElement.addElement("calcresource");
			Element memtotalElement = calcresourceElement.addElement("memtotal");
			Element memusedElement = calcresourceElement.addElement("memused");
			Element memavailElement = calcresourceElement.addElement("memavail");
			Element cputotalElement = calcresourceElement.addElement("cputotal");
			Element cpuusedElement = calcresourceElement.addElement("cpuused");
			Element cpuavailElement = calcresourceElement.addElement("cpuavail");		
			JSONObject queue_json = queues_json.getJSONObject(i);	
			String queueName = queue_json.getString("queueName");
			String maxApplications = queue_json.getString("maxApplications");
			String numActiveApplications = queue_json.getString("numActiveApplications");
			String numPendingApplications = queue_json.getString("numPendingApplications");
			double capacity = Double.parseDouble(queue_json.getString("capacity"));
			double maxCapacity = Double.parseDouble(queue_json.getString("maxCapacity")); 
			double userLimitFactor = Double.parseDouble(queue_json.getString("userLimitFactor"));
			double memused =  queue_json.getJSONObject("resourcesUsed").getDouble("memory");
			double cpuused =  queue_json.getJSONObject("resourcesUsed").getDouble("vCores");
			String type = "capacity";
			double userLimitCapacity = capacity * userLimitFactor;
			double memtotal =0.0;
			if (maxCapacity < userLimitCapacity) {
				memtotal = cmUtil.calculateTotalMemory();
			} else {
			    memtotal = (cmUtil.calculateTotalMemory() * capacity * userLimitFactor) / 100;
			}
			memtotal = Math.ceil(memtotal);
			double memavail = memtotal - memused;
			double cputotal = (cmUtil.calculateTotalCpu() * capacity * userLimitFactor) / 100;
			cputotal = Math.ceil(cputotal);
			double cpuavail = cputotal - cpuused;
			String numfinishedapps = String.valueOf(yarnUtil.getNumFinishedApps(queueName));
			
			nameElement.setText(queueName);
			maxappsElement.setText(maxApplications);
			numactiveappsElement.setText(numActiveApplications);
			numpendingappsElement.setText(numPendingApplications);	
			numfinishedappsElement.setText(numfinishedapps);
			typeElement.setText(type);
			userLimitFactorElement.setText(String.valueOf(userLimitFactor));
			memtotalElement.setText(String.valueOf(memtotal));
			memusedElement.setText(String.valueOf(memused));
			memavailElement.setText(String.valueOf(memavail));
			cputotalElement.setText(String.valueOf(cputotal));
			cpuusedElement.setText(String.valueOf(cpuused));
			cpuavailElement.setText(String.valueOf(cpuavail));	
		}
		return queuedescsElement;
	}
	
	/**
	 * 更改队列配置，用于修改队列接口
	 * @param queueConf 原队列配置
	 * @param queue 需要修改的队列名称
	 * @param newQueueCapa 新的队列capcity
	 * @param userLimitFactor capacity调度器配置中的userLimitFactor参数
	 * @return 返回修改后队列配置信息
	 * @throws Exception
	 */
	private List<Object> updateQueueConf_upateQueue(String queueConf, String queueName,
			double newQueueCapa, double userLimitFactor) throws Exception {
		List<Object> list = new ArrayList<Object>();
		String strXML = "";
		Document document = DocumentHelper.parseText(queueConf); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator();// 获取根节点下的子节点
		double defaultQueueCapaChange = 0;
		double userLimitFactorOld = 1;
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			String updateQueueCapa = "yarn.scheduler.capacity.root." + queueName + ".capacity";
			String updateQueueUserLimit = "yarn.scheduler.capacity.root." + queueName + ".user-limit-factor";
			String updateQueueMaxCapa = "yarn.scheduler.capacity.root." + queueName + ".maximum-capacity";
			if (updateQueueCapa.equals(node2.elementText("name"))) {
				double oldQueueCapa = Double.parseDouble(node2.elementText("value"));
				defaultQueueCapaChange = newQueueCapa - oldQueueCapa;
			    node2.element("value").setText(String.valueOf(newQueueCapa));
			}
			if (updateQueueUserLimit.equals(node2.elementText("name"))) {
				userLimitFactorOld= Double.parseDouble(node2.elementText("value"));
				node2.element("value").setText(String.valueOf(userLimitFactor));
			}
			if (updateQueueMaxCapa.equals(node2.elementText("value"))){
			    double maxCapacity = newQueueCapa * userLimitFactor;
			    node2.element("value").setText(String.valueOf(maxCapacity));
			}
		}
		Iterator<?> iter2 = node1.elementIterator();
		while (iter2.hasNext()) {
			Element node2 = (Element) iter2.next();
			if ("yarn.scheduler.capacity.root.default.capacity".equals(node2.elementText("name"))) {
				double newDefaultCapa = Double.parseDouble(node2.elementText("value")) - defaultQueueCapaChange;
				if (newDefaultCapa < 0) {
					list.add("failed");
					return list;
				}
				node2.element("value").setText(String.valueOf(newDefaultCapa));
			}
			StringWriter strWtr = new StringWriter();
			XMLWriter xmlWriter = new XMLWriter(strWtr);
			xmlWriter.write(document);
			strXML = strWtr.toString();
		}
		
		list.add(strXML);
		if (defaultQueueCapaChange != 0) {
			list.add("capacity");
			list.add(newQueueCapa);
		} else if (userLimitFactorOld != userLimitFactor) {
			list.add("userLimitFactor");
			list.add(userLimitFactor);
		}
		return list;
	}
	
	/**
	 * 修改队列配置信息，用于创建队列接口
	 * @param queueConf 原队列配置信息
	 * @param queueName 需要修改的队列名称
	 * @param newQueueCapa 队列新的capacity
	 * @param userLimitFactor capacity调度器配置中的userLimitFactor参数
	 * @return 返回修改后队列配置信息
	 * @throws Exception
	 */
	private String updateQueueConf_createQueue(String queueConf, String queueName,
			double newQueueCapa, double userLimitFactor) throws Exception {
		Document document = DocumentHelper.parseText(queueConf); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator();// 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if ("yarn.scheduler.capacity.root.default.capacity".equals(node2.elementText("name"))) {
				double newDefaultCapa = Double.parseDouble(node2.elementText("value")) - newQueueCapa;
				if (newDefaultCapa < 0) {
					return "failed";
				}
				node2.element("value").setText(String.valueOf(newDefaultCapa));
			}
			if ("yarn.scheduler.capacity.root.queues".equals(node2.elementText("name"))) {
				String newQueues = node2.elementText("value") + "," + queueName;
				node2.element("value").setText(newQueues);
			}
		}
		Element property1 = node1.addElement("property");
		Element name1 = property1.addElement("name");
		name1.setText("yarn.scheduler.capacity.root." + queueName + ".capacity");
		Element value1 = property1.addElement("value");
		value1.setText(String.valueOf(newQueueCapa));
		
		Element property2 = node1.addElement("property");
		Element name2 = property2.addElement("name");
		name2.setText("yarn.scheduler.capacity.root." + queueName + ".user-limit-factor");
		Element value2 = property2.addElement("value");
		value2.setText(String.valueOf(userLimitFactor));
		
		Element property3 = node1.addElement("property");
		Element name3 = property3.addElement("name");
		name3.setText("yarn.scheduler.capacity.root." + queueName + ".maximum-capacity");
		Element value3 = property3.addElement("value");
		double maxCapacity = newQueueCapa * userLimitFactor;
		maxCapacity = maxCapacity < 100 ? maxCapacity : 100;
		value3.setText(String.valueOf(maxCapacity));
	    
		Element property4 = node1.addElement("property");
		Element name4 = property4.addElement("name");
		name4.setText("yarn.scheduler.capacity.root." + queueName + ".state");
		Element value4 = property4.addElement("value");
		value4.setText("RUNNING");
		
		return JavaUtil.formatXML(document);
	}
	
    /**
     * 修改队列配置信息，用于启动、停止队列接口
	 * @param queueConf 原队列配置信息
	 * @param queueName 需要修改的队列名称
	 * @param newQueueCapa 队列新的capacity
	 * @param userLimitFactor capacity调度器配置中的userLimitFactor参数
	 * @return 返回修改后队列配置信息
	 * @throws Exception
     */
	private String updateQueueConf_stop_start(String queueConf, String queueName, String state) throws Exception{
		Document document = DocumentHelper.parseText(queueConf); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator();// 获取根节点下的子节点
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			String updateQueuestate = "yarn.scheduler.capacity.root." + queueName + ".state";
			if (updateQueuestate.equals(node2.elementText("name"))) {
			    node2.element("value").setText(state);
			}
		}
		return JavaUtil.formatXML(document);
	}
	
	/**
	 * 修改队列配置信息，用于删除队列接口
	 * @param queueConf 原队列配置信息
	 * @param queueName 需要修改的队列名称
	 * @param newQueueCapa 队列新的capacity
	 * @param userLimitFactor capacity调度器配置中的userLimitFactor参数
	 * @return 返回修改后队列配置信息
	 * @throws Exception
	 */
	private String updateQueueConf_deleteQueue(String queueConf, String queueName) throws Exception{
		Document document = DocumentHelper.parseText(queueConf); // 将字符串转化为xml
		Element node1 = document.getRootElement(); // 获得根节点
		Iterator<?> iter1 = node1.elementIterator();// 获取根节点下的子节点
		String rootQueue = "yarn.scheduler.capacity.root.queues";
		String defaultQueueCapa = "yarn.scheduler.capacity.root.default.capacity";
		String updateQueueCapa = "yarn.scheduler.capacity.root." + queueName + ".capacity";
		String updateQueueUserLimit = "yarn.scheduler.capacity.root." + queueName + ".user-limit-factor";
		String updateQueueMaxCapa = "yarn.scheduler.capacity.root." + queueName + ".maximum-capacity";
		String updateQueuestate = "yarn.scheduler.capacity.root." + queueName + ".state";
		double updatequeueCapaValue = 0;
		double defaultQueueCapaValue = 0;
		while (iter1.hasNext()) {
			Element node2 = (Element) iter1.next();
			if (rootQueue.equals(node2.elementText("name"))) {
			    String[] arr = node2.elementText("value").split(",");
			    List<String> queues = new ArrayList<String>(Arrays.asList(arr));
			    if (queues.contains(queueName)) {
			    	queues.remove(queueName);
			    }
			    String newQueues = StringUtils.join(queues.toArray(),",");
			    node2.element("value").setText(newQueues);
			}
			if (defaultQueueCapa.equals(node2.elementText("name"))) {
				defaultQueueCapaValue = Double.parseDouble(node2.elementText("value"));
			    continue;
			}
			if (updateQueueCapa.equals(node2.elementText("name"))) {
				updatequeueCapaValue = Double.parseDouble(node2.elementText("value"));
			    node1.remove(node2);
			    continue;
			}
			if (updateQueueUserLimit.equals(node2.elementText("name"))) {
			    node1.remove(node2);
			    continue;
			}
			if (updateQueueMaxCapa.equals(node2.elementText("name"))) {
			    node1.remove(node2);
			    continue;
			}
			if (updateQueuestate.equals(node2.elementText("name"))) {
			    node1.remove(node2);
			    continue;
			}
		}
		
		Iterator<?> iter1_1 = node1.elementIterator();
		double newDefaultQueueCapaValue = defaultQueueCapaValue + updatequeueCapaValue;
		while (iter1_1.hasNext()) { //将删除queue的capacity值加到default队列
			Element node2 = (Element) iter1_1.next();
			if (defaultQueueCapa.equals(node2.elementText("name"))) {
			    node2.element("value").setText(String.valueOf(newDefaultQueueCapaValue));
			    break;
			}
		}
		return JavaUtil.formatXML(document);
	}
	
	/**
	 * 修改角色配置组中的队列配置
	 * @param config 角色配置组信息
	 * @param queueConf 队列信息
	 * @return 返回修改后的角色组配置信息
	 * @throws Exception
	 */
	private String updateConfig(String config, String queueConf) throws Exception {
		JSONObject jsonobject = new JSONObject(config);
		JSONArray jsonarray = jsonobject.getJSONArray("items");
		for(int i = 0; i < jsonarray.length(); i++) {
			JSONObject jsonobject1 = jsonarray.getJSONObject(i);
			if ("resourcemanager_capacity_scheduler_configuration".equals(jsonobject1.getString("name"))) {
				jsonobject1.put("value", queueConf);				
			}
		}
		return jsonobject.toString();
	}
}
