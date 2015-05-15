package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import iie.ihadoop.util.InitConfig;

public class HiveMDAO {

	private static final String URL = InitConfig.URLHIVEMETADATA;
	private static final String USER = InitConfig.USERHIVEMETADATA;
	private static final String PSW = InitConfig.PASSHIVEMETADATA;
	
	public Connection  getconn(){
		Connection  conn=null;	  
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			conn=DriverManager.getConnection(URL,USER,PSW);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public boolean hasFunc(String funcName) {
		boolean flag = false;
		String sql = "select * from FUNCS where FUNC_NAME = '" + funcName + "'";
		System.out.println("sql>>>>>>>>>>>>>>>>"+sql);
		Connection  conn = getconn();
		try {
			Statement statement =conn.createStatement();
			ResultSet rs = statement.executeQuery(sql);
			System.out.println("rs>>>>>>>>>>>>>"+rs);
			while(rs.next()) {
				flag = true;
			}
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("flag>>>>>>>>>>>" + flag);
		return flag;
	}
	
}
