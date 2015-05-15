package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import iie.ihadoop.model.Operator;

public class OperatorDAO {
	/**
	 * 检测是否存在opid
	 * @param opid 算子唯一标识
	 * @return 存在返回true，不存在返回false
	 * @throws Exception
	 */
	public boolean existOpid(String opid) throws Exception {
		boolean existOpid = false;
		String querySql = "select opid from operator where opid = '" + opid + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(querySql);
		while(rs.next()) {
			existOpid = true;
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return existOpid;
	}
	
	/**
	 * 注册新的算子
	 * @param op 算子对象
	 * @throws Exception
	 */
	public void insertOperator(Operator op) throws Exception {
		String registerSql = "insert into operator(opid, version, biztype, provider, enginename, devlang, opjarpath, descriptor, instruction, registerTime) values('"
				+ op.getOpid() + "','" + op.getVersion() + "','" + op.getBiztype() + "','"
				+ op.getProvider() + "','" + op.getEnginename() + "','" + op.getDevlang()
				+ "','" + op.getOpjarpath() + "','" + op.getDescriptor() + "','"
				+ op.getInstruction() + "','" + op.getRegisterTime() + "')";
		MySqlDBManager.execSQL(registerSql);
	}
	
	/**
	 * 更新算子
	 * @param op 算子对象
	 * @throws Exception
	 */
	public void updateOperator(Operator op) throws Exception {
		String updateSql = "update operator set version = '" + op.getVersion() + "', biztype = '" + op.getBiztype() + "', provider = '" + op.getProvider() 
				+ "', enginename = '"+ op.getEnginename() + "', devlang = '" + op.getDevlang() + "', opjarpath = '"
				+ op.getOpjarpath() + "', descriptor = '" + op.getDescriptor()
				+ "', instruction = '" + op.getInstruction() + "' where opid = '" + op.getOpid() + "'";
		MySqlDBManager.execSQL(updateSql);
	}

	/**
	 * 获取算子包在hdfs的路径
	 * @param opid 算子id
	 * @return 返回hdfs路径字符串
	 * @throws Exception
	 */
	public String getOpjarPath(String opid) throws Exception {
		String opjarPath = "";
		String sql = "select opjarpath from operator where opid = '" + opid + "'"; 
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			opjarPath = rs.getString("opjarpath");
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return opjarPath;
	}
	
	/**
	 * 获得某算子的信息
	 * @param opid 算子id
	 * @return 返回算子实体对象
	 * @throws Exception
	 */
	public Operator getOperator(String opid) throws Exception {
		Operator op = new Operator();
		String sql = "select * from operator where opid = '" + opid + "'"; 
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = null;
		rs = statement.executeQuery(sql);
		while(rs.next()) {
			op.setEnginename(rs.getString("enginename"));
			op.setDescriptor(rs.getString("descriptor"));
			op.setDevlang(rs.getString("devlang"));
			op.setInstruction(rs.getString("instruction"));
			op.setOpid(opid);
			op.setOpjarpath(rs.getString("opjarpath"));
			op.setProvider(rs.getString("provider"));
			op.setRegisterTime(rs.getString("registerTime"));
			op.setVersion(rs.getString("version"));
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return op;
	}
	
	/**
	 * 删除算子
	 * @param opid 算子id
	 * @throws Exception
	 */
	public void deleteOperator(String opid) throws Exception {
		String deleteSql = "delete from operator where opid = '" + opid + "'";
		MySqlDBManager.execSQL(deleteSql);
	}
}
