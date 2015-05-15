package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import iie.ihadoop.model.Udf;

public class UdfDAO {

	/**
	 * 获得所有注册的udfname
	 * @return 返回udfnames列表
	 * @throws Exception
	 */
	public List<String> getUdfnames() throws Exception {
		List<String> list = new ArrayList<String>();
		String sql = "select udfname from udf";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			String udfname = rs.getString("udfname");
			list.add(udfname);
		}
		rs.close();
		statement.close();
		return list;
	}
	
	/**
	 * 通过udf名称获得udf对象
	 * @param udfname udf名称
	 * @return udf对象
	 * @throws Exception
	 */
	public Udf getUdfByUdfame(String udfname) throws Exception {
		Udf udf = new Udf();
		String sql = "select * from udf where udfname = '" + udfname + "'";
		Connection conn = MySqlDBManager.getConn();
		Statement statement =conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()) {
			udf.setAuthor(rs.getString("author"));
			udf.setClassname(rs.getString("classname"));
			udf.setDescriptor(rs.getString("descriptor"));
			udf.setRegistertime(rs.getString("registertime"));
			udf.setjarname(rs.getString("jarname"));
			udf.setUdfname(udfname);
		}
		rs.close();
		statement.close();
		MySqlDBManager.closeConn(conn);
		return udf;
	}
	
	/**
	 * 注册新的udf记录
	 * @param udf udf对象
	 * @throws Exception
	 */
	public void insertUdf(Udf udf) throws Exception {
		String registerSql = "insert into udf(udfname, classname, jarname, descriptor, author, registertime) values('"
				 + udf.getUdfname() + "','" + udf.getClassname() + "','" + udf.getjarname() + "','"
				 + udf.getDescriptor() + "','" + udf.getAuthor() + "','" + udf.getRegistertime() + "')";
		MySqlDBManager.execSQL(registerSql);
	}
	
	/**
	 * 删除udf记录
	 * @param udfname udf名称
	 * @throws Exception
	 */
	public void deleteUdf(String udfname) throws Exception {
		String deleteSql = "delete from udf where udfname = '" + udfname + "'";
		MySqlDBManager.execSQL(deleteSql);
	}
	
	/**
	 * 更新udf信息
	 * @param udf udf对象
	 * @throws Exception
	 */
	public void updateUdf(Udf udf) throws Exception {
		String updateSql = "update udf set descriptor = '" + udf.getDescriptor() + "', author = '" 
	             + udf.getAuthor() + "' where udfname = '" + udf.getUdfname() + "'";
		MySqlDBManager.execSQL(updateSql);
	}
}
