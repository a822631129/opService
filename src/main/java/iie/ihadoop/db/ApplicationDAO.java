package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import iie.ihadoop.model.ApplicationInstance;


public class ApplicationDAO {	

	/**
	 * 通过算子任务id获得application表中处于运行状态的记录
	 * @param otid 算子任务id
	 * @return 返回running的application id
	 * @throws Exception
	 */
	public List<String> getRunAppidsByOtid(String otid) throws Exception {
		List<String> list = new ArrayList<String>();
		String sql = "select status, appid from apptask where otid = '" + otid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		String status = "";
		while(rs.next()) {
			status = rs.getString("status");
			if (!"SUCCEEDED".equals(status) && !"FAILED".equals(status)) {
				String appid = rs.getString("appid");
				list.add(appid);
			}
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return list;
	}

	
	/**
	 * 通过算子任务id获得application表中同一otid的application的记录
	 * @param otid 算子任务id
	 * @return 返回同一所有的application id
	 * @throws Exception
	 */
	public List<String> getAllAppidsByOtid(String otid) throws Exception {
		List<String> list = new ArrayList<String>();
		String sql = "select appid from apptask where otid = '" + otid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while (rs.next()) {
			String appid = rs.getString("appid");
			list.add(appid);
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return list;
	}
	
	/**
	 * 插入新的Application记录
	 * @param op 算子对象
	 * @throws Exception
	 */
	public void insertApplication(ApplicationInstance app) throws Exception {
		String insertAppSql = "insert into apptask(appid,otid,sequence,status,startTime) values('"
				+ app.getAppid() + "','" + app.getOtid() + "'," + app.getSquence() + ",'"
				+ app.getStatus() + "','" + app.getStartTime() + "')";
		MySqlDBManager.execSQL(insertAppSql);
	}
	
	/**
	 * 通过appid更改Application状态
	 * @param status 新状态
	 * @param appid Application唯一标识
	 * @throws Exception 
	 */
	public void updateApplication(String status, String appid) throws Exception {
		String updateAppSql = "update apptask set status = '" + status + "' where appid = '" + appid + "'";
		MySqlDBManager.execSQL(updateAppSql);
	}
}
