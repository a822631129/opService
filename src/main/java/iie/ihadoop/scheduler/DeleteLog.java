package iie.ihadoop.scheduler;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import iie.ihadoop.util.HdfsUtil;
import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;

/**
 * 定时清理过期的本地和hdfs日志文件
 * 
 * @author sunyang
 * 
 */
public class DeleteLog implements Job{
	@Override  
    public void execute(JobExecutionContext arg0) throws JobExecutionException {  
        System.out.println("执行删除.......deleteLocalLog" + JavaUtil.getTime());  
        deleteLocalLog(InitConfig.REQUESTFILEPATH); // 删除算子提交任务接口中的请求信息文件
        deleteLocalLog(InitConfig.REQUESTFILEPATH_OTHER); //删除其他接口的请求信息文件
        deleteLocalLog(InitConfig.STDERR); //删除stderr文件
        deleteLocalLog(InitConfig.STDOUT); //删除stdout文件
        
        System.out.println("执行删除.......deleteHdfsLog" + JavaUtil.getTime());
        deleteHdfsLog(InitConfig.HDFSAPPLOGS); //删除hdfs上执行Application的container日志
    }  
	
	public static void deleteLocalLog(String dirPath){
		Calendar cdweek = Calendar.getInstance();
		cdweek.add(Calendar.DATE, InitConfig.DELETELOG_DAY); 
		Date d = cdweek.getTime();
		
		File fileBag =new File(dirPath);
		if (fileBag.exists()) {
			String[] filesName = fileBag.list();
			for (int i = 0; i < filesName.length; i++) {
				System.out.println(filesName[i]);
				File file = new File(dirPath + filesName[i]);
				Long time = file.lastModified(); // 获得文件的修改时间
				Calendar cd = Calendar.getInstance();
				cd.setTimeInMillis(time);
				Date fileDate = cd.getTime();
				boolean flag = fileDate.before(d); //比较过期文件
				if (flag) {
					System.out.println("begin delete LocalLog>>>>>>>>>>>>>");
					System.out.println("是否删除：" + JavaUtil.deleteDir(dirPath + filesName[i]));
				} else {
					System.out.println(filesName[i] + "：是" + InitConfig.DELETELOG_DAY + "天内创建的文件，不需要删除");
				}
			}
		} else {
			System.out.println( "the dir " + dirPath + "is not exist");
		}
	}
	
	public void deleteHdfsLog(String dirPath) {
		HdfsUtil hdfsUtil = new HdfsUtil();
        try {
			hdfsUtil.deleteDirAllFiles(dirPath);
			System.out.println("begin delete HdfsLog>>>>>>>>>>>>>");
			System.out.println("是否删除：" + hdfsUtil.deleteDirAllFiles(dirPath));
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
}
