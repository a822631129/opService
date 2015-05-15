package iie.ihadoop.scheduler;

import java.io.File;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import iie.ihadoop.util.InitConfig;
import iie.ihadoop.util.JavaUtil;

/**
 * 删除本地缓存的算子zip包
 * 
 * @author sunyang
 *
 */
public class DeleteOpTemp implements Job{
	@Override  
    public void execute(JobExecutionContext arg0) throws JobExecutionException {  
        System.out.println("执行删除算子缓存......." + JavaUtil.getTime());  
        deleteOpTemp(InitConfig.BASEPATH + "temp/"); //删除算子缓存压缩包
    }  
	
	public static void deleteOpTemp(String dirPath) {
		File fileBag = new File(dirPath);
		if (fileBag.exists()) {
			String[] filesName = fileBag.list();
			for (int i = 0; i < filesName.length; i++) {
				System.out.println("begin delete op temp>>>>>>>>>>>>>" + filesName[i]);
				System.out.println("是否删除：" + JavaUtil.deleteDir(dirPath + filesName[i]));
			}
		} else {
			System.out.println("the dir " + dirPath + "is not exist");
		}
	}
	
}
