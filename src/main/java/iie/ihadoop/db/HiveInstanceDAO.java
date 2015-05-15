package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveInstanceDAO {

	/**
	 * 通过otid检验是否存在hive实例
	 * @param otid 算子任务id
	 * @return 
	 * @throws Exception
	 */
	public boolean existHiveInstByOtid(String otid) throws Exception {
		boolean flag = false;
		String sql = "select * from hiveinstance where otid = '" + otid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			flag = true;
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return flag;
	}
	
	/**
	 * 通过otid获得执行hive任务实例的句柄
	 * @param otid 算子任务id
	 * @return 
	 * @throws Exception
	 */
	public String getDetailByOtid(String otid) throws Exception {
		String sql = "select detail from hiveinstance where otid = '" + otid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		String detail = "";
		while(rs.next()) {
			detail = rs.getString("detail");
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return detail;
	}
	
	/**
	 * 更新hive任务实例的状态
	 * @param otid 算子任务id
	 * @param status 新状态
	 * @throws Exception
	 */
	public void updateHiveInstanceStatus(String otid, String status) throws Exception {
		String deleteSql = "update hiveinstance set status = '" + status + "' where otid = '" + otid + "'";
		MySqlDBManager.execSQL(deleteSql);
	}

}
