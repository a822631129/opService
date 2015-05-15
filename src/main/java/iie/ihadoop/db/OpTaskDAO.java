package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import iie.ihadoop.model.OpTask;

/**
 * mysql访问接口
 */
public class OpTaskDAO {
	
	/**
	 * 通过otid获得算子任务状态
	 * @param otid 算子任务id
	 * @return 返回算子任务状态和提交时间
	 * @throws Exception
	 */
	public List<String> getOptaskStatus(String otid) throws Exception {
		List<String> list = new ArrayList<String>();
		String sql = "select status, submitTime from optask where otid = '" + otid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		String status = "";
		String submitTime = "";
		while(rs.next()) {
			status = rs.getString("status");
			submitTime = rs.getString("submitTime");
			list.add(status);
			list.add(submitTime);
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return list;
	}
	
	/**
	 * 在结束同步提交算子任务接口中，通过场景实例id和算子id获得算子任务id
	 * @param jobinstaceid 场景实例id
	 * @param opid 算子id
	 * @return 算子任务id
	 * @throws Exception
	 */
	public String getOtid(String jobinstaceid, String opid) throws Exception {
		String sql = "select otid from optask where opid = '" + opid + "' and jobInstanceId = '" + jobinstaceid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		String otid = "";
		while(rs.next()) {
			otid = rs.getString("otid");
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return otid;
	}
	
	/**
	 * 插入算子任务记录
	 * @throws Exception 
	 */
	public void insertOpTask(OpTask ot) throws Exception {
		String insertOptaskSql = "insert into optask(otid,opid,processId,jobInstanceId,userName,queueName,submitTime,submission,status) values('"
				+ ot.getOtid() + "','" + ot.getOpid() + "','" + ot.getProcessId() + "','" + ot.getJobInstanceId() + "','" + ot.getUserName()
				+ "','" + ot.getQueueName() + "','" + ot.getSubmitTime() + "','" + ot.getSubmission() + "','" + ot.getStatus() + "')";
		MySqlDBManager.execSQL(insertOptaskSql);
	}
	
	/**
	 * 通过otid更新算子任务状态
	 * @param status 新状态
	 * @param otid 算子任务id
	 * @throws Exception
	 */
	public void updateOpTaskStatus(String status, String otid) throws Exception {
		String updateOtSql = "update optask set status = '" + status + "' where otid = '" + otid + "'";
		MySqlDBManager.execSQL(updateOtSql);
	}
}
