package iie.ihadoop.util;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;





//import org.dom4j.Document;
//import org.dom4j.DocumentException;
//import org.dom4j.Element;
//import org.dom4j.io.SAXReader;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
//import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import iie.ihadoop.scheduler.DeleteLog;
import iie.ihadoop.scheduler.DeleteOpTemp;

public class InitConfig extends HttpServlet implements Servlet{
	//httpservlet 实现了serializable 接口。所以这里的id的作用就是用来标识这个对象写入文件后的标识。
	private static final long serialVersionUID = 3994980056352080271L; 
	
	public static final String BASEPATH = ConfigContext.getInstance().getString("basePath"); // 算子父目录
	public static final String STDERR = ConfigContext.getInstance().getString("stderr"); // 标准输出信息保存文件路径
	public static final String STDOUT = ConfigContext.getInstance().getString("stdout"); // 标准错误信息保存文件路径
	public static final String APPID =  ConfigContext.getInstance().getString("appid"); //applicatonid信息保存文件路径
	public static final String LOCALAPPLOGS =  ConfigContext.getInstance().getString("localAppLogs"); //application的日志文件本地保存地址
	public static final String HDFSAPPLOGS =  ConfigContext.getInstance().getString("hdfsAppLogs"); //application的日志文件hdfs保存地址
	//public static final String OPSERVICELOGS =  ConfigContext.getInstance().getString("opServieLogs"); //相关
	public static final String REQUESTFILEPATH = ConfigContext.getInstance().getString("requestFilePath"); // 接收的xml参数保存文件路径
	public static final String REQUESTFILEPATH_OTHER = ConfigContext.getInstance().getString("requestFilePathOther"); // 接收的xml参数保存文件路径
	public static final String CRON_DELETEOPTEMP = ConfigContext.getInstance().getString("cronDeleteOpTemp"); //删除算子包缓存定时任务表达式
	public static final String CRON_DELETELOG = ConfigContext.getInstance().getString("cronDeleteLog"); //删除日志文件定时任务表达式
	public static final int DELETELOG_DAY = ConfigContext.getInstance().getInteger("deleteLogDay"); //删除多少天之前的日志文件
	public static final String CHECKSUM_DIR = ConfigContext.getInstance().getString("checksumDir"); //校验码目录名

	public static final String URLMYSQL = ConfigContext.getInstance().getString("mysql.url"); //mysql地址
	public static final String USERMYSQL = ConfigContext.getInstance().getString("mysql.user"); //用户名
	public static final String PASSMYSQL = ConfigContext.getInstance().getString("mysql.psw"); //密码
	
	public static final String URLHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.url"); //hive元数据库地址
	public static final String USERHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.user"); //用户名
	public static final String PASSHIVEMETADATA = ConfigContext.getInstance().getString("hiveMetaData.psw"); //密码
	
	public static final String CM_ADDR = ConfigContext.getInstance().getString("cm.url"); //cm界面地址
	public static final String CM_USER = ConfigContext.getInstance().getString("cm.user"); //用户名
	public static final String CM_PSW = ConfigContext.getInstance().getString("cm.psw"); //密码

//	public static final String ROLESURL = ConfigContext.getInstance().getString("rolesURL");
//	public static final String ROLECONFIGGROUPSURL = ConfigContext.getInstance().getString("roleConfigGroupsURL");
//	public static final String GROUPCONFIGURL = ConfigContext.getInstance().getString("groupConfigURL");
	
	public static final long MAXQUOTA = ConfigContext.getInstance().getLong("maxQuota"); // 最大配额 50GB*1073741824
	public static final String ROOT_PATH = ConfigContext.getInstance().getString("rootPath"); // 存储资源hdfs根目录

	public static final String FUNCTIONJARPATH = ConfigContext.getInstance().getString("functionJarPath"); //udf包在hdfs的保存路径
	
	public InitConfig() throws Exception{
		
	}
	
	private Scheduler getScheduler() throws Exception {
		SchedulerFactory schedFact = new StdSchedulerFactory();
        Scheduler sched = schedFact.getScheduler();
		return sched;
	}
	
	Scheduler sched = getScheduler();

	public void init() throws ServletException {
		try {
			JobDetail deleteTemp_job = new JobDetail("deleteOpTemp_job", "deleteTemp_jobGroup", DeleteOpTemp.class ); 
	        CronTrigger deleteTemp_trig = new CronTrigger("deleteOpTemp_trig", "deleteTemp_trigGroup");      
	        //deleteTemp_trig.setCronExpression("0 0 23 * * ?");  
	        deleteTemp_trig.setCronExpression(CRON_DELETEOPTEMP); //设置调度规则
	        sched.scheduleJob(deleteTemp_job, deleteTemp_trig);;
	        
	        JobDetail sql_job = new JobDetail("deleteLog_job", "sql_jobGroup", DeleteLog.class ); 
	        CronTrigger sql_trig = new CronTrigger("deleteLog_trig", "sql_trigGroup");  
	        sql_trig.setCronExpression(CRON_DELETELOG); 
	        sched.scheduleJob(sql_job, sql_trig);
			sched.start();  //定时调度任务
	        System.out.println("启动定时任务");
		} catch (Exception e) {
			e.printStackTrace();
		} 
	
	}
	
	public void destroy() {
		try {
			System.out.println("start destroy >>>>>>>>>>>>>>>>>>>>>");
			sched.shutdown();
			System.out.println("停掉定时任务");
		}  catch (Exception e) {
			e.printStackTrace();
		}
	}

//	private void initParams() {
//		System.out.println(">>初始化配置参数>>>>>>>>>>>>>>>>>>>>>");
//		String ConfFilePath = this.getClass().getClassLoader()
//				.getResource("opServiceConf.xml").toString();
//		ConfFilePath = ConfFilePath.substring(5);
//		System.out.println(">>>>>>>>>配置文件路徑：" + ConfFilePath);
//		File file = new File(ConfFilePath);
//		SAXReader saxReader = new SAXReader();
//		Document document = null;
//		try {
//			document = saxReader.read(file);
//		} catch (DocumentException e) {
//			e.printStackTrace();
//		}
//		Element node1 = document.getRootElement(); // 获得根节点
//		Iterator iter1 = node1.elementIterator(); // 获取根节点下的子节点
//		while (iter1.hasNext()) {
//			Element node2 = (Element) iter1.next();
//			if ("requestFilePath".equals(node2.getName())) {
//				REQUESTFILEPATH = node2.getText();
//			}
//			if ("requestFilePathOther".equals(node2.getName())) {
//				REQUESTFILEPATH_OTHER = node2.getText();
//			}
//			if ("cronExpression".equals(node2.getName())) {
//				CRON_EXPRESSION = node2.getText();
//			}
//			if ("deleteLogDay".equals(node2.getName())) {
//				DELETELOG_DAY = Integer.parseInt(node2.getText());
//			}
//			if ("storagespace".equals(node2.getName())) {
//				Iterator iter2_s = node2.elementIterator();
//				while (iter2_s.hasNext()) {
//					Element node3 = (Element) iter2_s.next();
//					if ("maxquota".equals(node3.getName())) {
//						String maxquota_temp = node3.getText().substring(0,
//								node3.getText().length() - 2);
//						MAXQUOTA = Long.parseLong(maxquota_temp) * 1024 * 1024 * 1024;
//					}
//					if ("defaultquota".equals(node3.getName())) {
//						DEFAULTQUOTA = node3.getText().substring(0,
//								node3.getText().length() - 2);
//					}
//					if ("rootpath".equals(node3.getName())) {
//						ROOT_PATH = node3.getText();
//					}
//				}
//			}
//			if ("optask".equals(node2.getName())) {
//				Iterator iter2 = node2.elementIterator();
//				while (iter2.hasNext()) {
//					Element node3 = (Element) iter2.next();
//					if ("basePath".equals(node3.getName())) {
//						BASEPATH = node3.getText();
//					}
//					if ("stderr".equals(node3.getName())) {
//						STDERR = node3.getText();
//					}
//					if ("stdout".equals(node3.getName())) {
//						STDOUT = node3.getText();
//					}
//					if ("appid".equals(node3.getName())) {
//						APPID = node3.getText();
//					}
//					if ("checksumDirName".equals(node3.getName())) {
//						CHECKSUM_DIR = node3.getText();
//					}
//				}
//			}
//			if ("dbaccess".equals(node2.getName())) {
//				Iterator iter2 = node2.elementIterator();
//				while (iter2.hasNext()) {
//					Element node3 = (Element) iter2.next();
//					if ("urlMysql".equals(node3.getName())) {
//						URLMYSQL = node3.getText();
//					}
//					if ("userMysql".equals(node3.getName())) {
//						USERMYSQL = node3.getText();
//					}
//					if ("passMysql".equals(node3.getName())) {
//						PASSMYSQL = node3.getText();
//					}
//				}
//			}
//			if ("cluster".equals(node2.getName())) {
//				Iterator iter2_d = node2.elementIterator();
//				while (iter2_d.hasNext()) {
//					Element node3 = (Element) iter2_d.next();
//					if ("rolesURL".equals(node3.getName())) {
//						ROLESURL = node3.getText();
//					}
//					if ("roleConfigGroupsURL".equals(node3.getName())) {
//						ROLECONFIGGROUPSURL = node3.getText();
//					}
//					if ("groupConfigURL".equals(node3.getName())) {
//						GROUPCONFIGURL = node3.getText();
//					}
//					if ("cmAddr".equals(node3.getName())) {
//						CM_ADDR = node3.getText();
//					}
//					if ("cmUser".equals(node3.getName())) {
//						CM_USER = node3.getText();
//					}
//					if ("cmPassword".equals(node3.getName())) {
//						CM_PSW = node3.getText();
//					}
//				}
//			}
//			if ("udf".equals(node2.getName())) {
//				Iterator iter2 = node2.elementIterator();
//				while (iter2.hasNext()) {
//					Element node3 = (Element) iter2.next();
//					if ("functionJarPath".equals(node3.getName())) {
//						FUNCTIONJARPATH = node3.getText();
//					}
////					if ("userMysql".equals(node3.getName())) {
////						userMysql = node3.getText();
////					}
////					if ("passMysql".equals(node3.getName())) {
////						passMysql = node3.getText();
////					}
//				}
//			}
//		}
//		System.out.println("==========================初始化配置==========================");
//	System.out.println("basePath = " + InitConfig.BASEPATH);
//	System.out.println("stdout = " + InitConfig.STDOUT);
//	System.out.println("stderr = " + InitConfig.STDERR);
//	System.out.println("appid = " + InitConfig.APPID);
//	
//	System.out.println("requestFilePath = " + InitConfig.REQUESTFILEPATH);
//	System.out.println("requestFilePathOther = " + InitConfig.REQUESTFILEPATH_OTHER);
//	System.out.println("cronExpression = " + InitConfig.CRON_EXPRESSION);
//	System.out.println("deleteLogDay = " + InitConfig.DELETELOG_DAY);
//	
//	System.out.println("checksumdir = " + InitConfig.CHECKSUM_DIR);
//	
//	System.out.println("urlMysql = " + InitConfig.URLMYSQL);
//	System.out.println("userMysql = " + InitConfig.USERMYSQL);
//	System.out.println("passMysql = " + InitConfig.PASSMYSQL);
//	
//	System.out.println("CM_ADDR = " + InitConfig.CM_ADDR);
//	System.out.println("CM_USER = " + InitConfig.CM_USER);
//	System.out.println("CM_PSW = " + InitConfig.CM_PSW);
//	
//	System.out.println("rolesURL = " + InitConfig.ROLESURL);
//	System.out.println("roleConfigGroupsURL = " + InitConfig.ROLECONFIGGROUPSURL);
//	System.out.println("groupConfigURL = " + InitConfig.GROUPCONFIGURL);
//	
//	System.out.println("maxquota = " + InitConfig.MAXQUOTA);
//	System.out.println("defaultquota = " + InitConfig.DEFAULTQUOTA);
//	System.out.println("rootpath = " + InitConfig.ROOT_PATH);
//	
//	System.out.println("functionJarPath = " + InitConfig.FUNCTIONJARPATH);
//		System.out.println("===========================================================");
//	
//	}

}
