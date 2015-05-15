package iie.ihadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

/**
 * hdfs工具类
 * @author sunyang
 *
 */
public class HdfsUtil {
	
	/**
	 * 删除文件，可是目录
	 * @param filePath 文件路径
	 * @return 删除成功与否
	 * @throws Exception
	 */
	public boolean deleteFile(String filePath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path path = new Path(filePath);
		boolean flag = fileSystem.delete(path, true); //true 可删除目录
		return flag;
	}
	
	/**
	 * 删除目录下所有文件，不包括目录本身
	 * @param dirPath 目录路径
	 * @return 删除成功与否
	 * @throws Exception
	 */
	public boolean deleteDirAllFiles(String dirPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path dirpath = new Path(dirPath);
		FileStatus[] filestatus = fileSystem.listStatus(dirpath);
		for (FileStatus filestatu : filestatus) {
			Path filepath = filestatu.getPath();
			boolean flag = fileSystem.delete(filepath, true);
			if (!flag) {
				return false;
			}
		}
		return true;
	}

	public boolean existFile(String filePath) throws Exception {
		boolean existFile = false;
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path filepath = new Path(filePath);
		if (fileSystem.exists(filepath)) {
			existFile = true;
		}
		return existFile;
	}
	
	/**
	 * 从hdfs上下载文件到本地
	 * @param hdfsPath 文件hdfs路径
	 * @param localPath 本地目标路径
	 * @throws Exception
	 */
	public void getFileFromHdfs(String hdfsPath, String localPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path hdfspath = new Path(hdfsPath);
		FSDataInputStream fsis = fileSystem.open(hdfspath);
		File localPathFile = new File(localPath);
		if (!localPathFile.getParentFile().exists()) {
			localPathFile.getParentFile().mkdirs();
		}
		OutputStream os = new FileOutputStream(localPathFile);
		IOUtils.copyBytes(fsis, os, 1024, true);
		fileSystem.close();
		/*buffSize - the size of the buffer
		close - whether or not close the InputStream and OutputStream at the end. 
		The streams are closed in the finally clause. */
	}	
	
	/**
	 * 获得hdfs文件内容信息
	 * @param hdfsPath 文件hdfs路径
	 * @return 返回String格式信息
	 * @throws Exception
	 */
	public String getStringFromHdfs(String hdfsPath) throws Exception {	
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path hdfspath = new Path(hdfsPath);
		FSDataInputStream fsin = fileSystem.open(hdfspath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fsin));
		String data = "";
		String dataTemp = "";
		while ((dataTemp = reader.readLine()) != null) {
			data += "\n" + dataTemp;
		}
		data = data.substring(1);
		reader.close();
		fsin.close();
		return data;
	}
	
	public String getSpaceQuota(String dirPath) throws Exception {
		Path dirpath = new Path(dirPath);
		FileSystem fileSystem = FileSystem.get(new Configuration());
		ContentSummary contsumary = fileSystem.getContentSummary(dirpath);
		String spaceQuota = String.valueOf(contsumary.getSpaceQuota());
		return spaceQuota;
	}
	
	/**
	 * 创建hdfs目录
	 * @param dirPath 目录路径
	 * @return 创建成功与否
	 * @throws Exception
	 */
	public boolean mkdirs(String dirPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path path = new Path(dirPath);
		boolean flag = fileSystem.mkdirs(path);
		return flag;
	}	
	
	/**
	 * 将本地文件上传到hdfs
	 * @param localPath 本地文件路径
	 * @param hdfsPath hdfs目标路径
	 * @throws Exception
	 */
	public void putFileToHdfs(String localPath, String hdfsPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path hdfspath = new Path(hdfsPath);
		FSDataOutputStream fsos = fileSystem.create(hdfspath);
		if (!fileSystem.exists(hdfspath.getParent())) {
			fileSystem.mkdirs(hdfspath.getParent());
		}
		InputStream is = new FileInputStream(localPath);
		IOUtils.copyBytes(is, fsos, 1024, true);
	}
	
	/**
	 * 将字符串信息写到hdfs文件中
	 * @param data 字符串信息
	 * @param hdfsPath hdfs文件路径
	 * @throws Exception
	 */
	public void putStringToHdfs(String data, String hdfsPath) throws Exception {
		FileSystem fileSystem = FileSystem.get(new Configuration());
		Path hdfspath = new Path(hdfsPath);
		if (!fileSystem.exists(hdfspath.getParent())) {
			fileSystem.mkdirs(hdfspath.getParent());
		}
		FSDataOutputStream out = fileSystem.create(hdfspath);
		InputStream fin = new ByteArrayInputStream(data.getBytes());
		IOUtils.copyBytes(fin, out, 1024, true);
	}
	
	
	/**
	 * 设置目录配额（目录内最大存放文件配额）
	 * @param dirPath 目录路径
	 * @param quota 配额大小
	 * @throws Exception
	 */
	public void setSpaceQuota(String dirPath, String quota) throws Exception {
		DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(new Configuration());
		Path path = new Path(dirPath);
		dfs.setQuota(path, Long.MAX_VALUE, Long.parseLong(quota)); // 设定配额
	}
	
}
