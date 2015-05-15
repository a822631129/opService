package iie.ihadoop.service;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.RMHAUtils;

import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;
/**
 * 测试接口，供测试使用
 * @author mashaolong, sunyang
 *
 */
@Path("/test")
public class OSTest {
	private static final Log log = LogFactory.getLog(OSTest.class);
	
	@GET
	@Path("/hello")
	public String hello(@Context HttpServletRequest request) throws Exception {
		System.out.println("三十秒前前前前前前前前前前前qianqianqianqian");
		Thread.sleep(new Long(30000));
		System.out.println("三十秒后后后后后后后houhouhouhouhou");
		return "测试程序 等了30秒 test~~~~~";
	}

	@GET
	@Path("/350hello")
	public String hello400(@Context HttpServletRequest request)
			throws Exception {
		System.out.println("==350秒前前前前前前前前前前前qianqianqianqian");
		Thread.sleep(new Long(350000));
		System.out.println("===350秒后后后后后后后houhouhouhouhou");
		return "测试程序 等了350秒 test~~~~~";
	}

	@GET
	@Path("/200hello")
	public String hello200(@Context HttpServletRequest request)
			throws Exception {
		System.out.println("==200秒前前前前前前前前前前前qianqianqianqian");
		Thread.sleep(new Long(200000));
		System.out.println("===200秒后后后后后后后houhouhouhouhou");
		return "测试程序 等了200秒 test~~~~~";
	}
	
	@GET
	@Path("/log/{appid}")
	public String getContainersLogs(@PathParam("appid") String appid) throws Exception {
		String appLogPath = InitConfig.LOCALAPPLOGS;
		Configuration config = new YarnConfiguration();
		org.apache.hadoop.fs.Path remoteRootLogDir = null;
		//Get the value of the name property as an int. If no such property exists, the provided default value is returned
		remoteRootLogDir = new org.apache.hadoop.fs.Path(config.get("yarn.nodemanager.remote-app-log-dir", "/tmp/logs"));
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(config);
		yarnClient.start();
		long cluster = Long.parseLong(appid.substring(12, 25));
		int id = Integer.parseInt(appid.substring(27));
		ApplicationId applicationid = ApplicationId.newInstance(cluster, id);
	    String user = yarnClient.getApplicationReport(applicationid).getUser();
	    String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(config);

	    org.apache.hadoop.fs.Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(remoteRootLogDir, applicationid, user, logDirSuffix);
	    RemoteIterator<?> nodeFiles;
	    try {
	    	org.apache.hadoop.fs.Path qualifiedLogDir = FileContext.getFileContext(config).makeQualified(remoteAppLogDir);
	        nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), config).listStatus(remoteAppLogDir);
	    } catch (FileNotFoundException fnf) {
	        JavaUtil.log("Logs not available at " + remoteAppLogDir.toString(), appLogPath);
	        JavaUtil.log("Log aggregation has not completed or is not enabled.", appLogPath);
	        return "FileNotFoundException";
	    }
	    
	    while (nodeFiles.hasNext()) {
	        FileStatus thisNodeFile = (FileStatus)nodeFiles.next();
	        AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(config, new org.apache.hadoop.fs.Path(remoteAppLogDir, thisNodeFile.getPath().getName()));
	        try {
	            AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
	            DataInputStream valueStream = reader.next(key);
	            while (valueStream != null) {
	                String containerString = "\n\nContainer: " + key + " on " + thisNodeFile.getPath().getName();

	                JavaUtil.log(containerString, appLogPath);
	                JavaUtil.log(StringUtils.repeat("=", containerString.length()), appLogPath);
	                try {
	                    while (true) {
	            	        readAContainerLogsForALogType(valueStream, appLogPath);
	                    }
	                } catch (EOFException eof) {
	                      key = new AggregatedLogFormat.LogKey();
	                      valueStream = reader.next(key);
	                  }
	            }
	        } finally { 
	        	reader.close(); 
	          }
	      }
		return "end>>>>>>>>>>>>>";
	}
	
	 public static void readAContainerLogsForALogType(DataInputStream valueStream, String appLogPath) throws Exception {
		      byte[] buf = new byte[65535];

		/**
		 *     .readUTF(),首先读取两个字节，并使用它们构造一个无符号 16 位整数，构造方式与 readUnsignedShort
		      方法的方式完全相同。该整数值被称为 UTF长度，它指定要读取的额外字节数。
		      然后成组地将这些字节转换为字符。每组的长度根据该组第一个字节的值计算
		      紧跟在某个组后面的字节（如果有）是下一组的第一个字节。
		 */
		      String fileType = valueStream.readUTF();
		      String fileLengthStr = valueStream.readUTF();
		      long fileLength = Long.parseLong(fileLengthStr);
		      JavaUtil.log("LogType: ", appLogPath);
		      JavaUtil.log(fileType, appLogPath);
		      JavaUtil.log("LogLength: ", appLogPath);
		      JavaUtil.log(fileLengthStr, appLogPath);
		      JavaUtil.log("Log Contents:", appLogPath);

		      long curRead = 0L;
		      long pendingRead = fileLength - curRead;
		      int toRead = pendingRead > buf.length ? buf.length : (int)pendingRead;

		      int len = valueStream.read(buf, 0, toRead);
		      OutputStream os = new FileOutputStream(appLogPath, true);
		      while ((len != -1) && (curRead < fileLength)) {
		    	os.write(buf, 0, len);
		        curRead += len;
		        pendingRead = fileLength - curRead;
		        toRead = pendingRead > buf.length ? buf.length : (int)pendingRead;
		        len = valueStream.read(buf, 0, toRead);
		      }
		      os.close();
		      JavaUtil.log("", appLogPath);
		    }
	 
	 @GET
	 @Path("/rmid")
	 public String getRMHAId() {
		 YarnConfiguration conf = new YarnConfiguration();
		 String rmid = RMHAUtils.findActiveRMHAId(conf);
		 return "rmid>>>>>>" + rmid;
	 }
	 
	 @GET
	 @Path("/webapp")
	 public String getRmWebappAddr() {
		 YarnConfiguration conf = new YarnConfiguration();
		 boolean isHA = HAUtil.isHAEnabled(conf);
		 log.info("isHA:" + isHA);
		 String rmAddr = "";
		 if (isHA) {
			 String rmid = RMHAUtils.findActiveRMHAId(conf);
			 rmAddr = conf.get("yarn.resourcemanager.webapp.address." + rmid);
		 } else {
			 rmAddr = conf.get("yarn.resourcemanager.webapp.address");
		 }
		 log.info("rmAddr:" + rmAddr);
		 return rmAddr;
	 }
}
