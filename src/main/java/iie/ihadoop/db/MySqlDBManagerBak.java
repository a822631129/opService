package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import iie.ihadoop.util.InitConfig;

public class MySqlDBManagerBak {
	private static final String URL = InitConfig.URLMYSQL;
	private static final String USER = InitConfig.USERMYSQL;
	private static final String PSW = InitConfig.PASSMYSQL;

	public Connection conn = null;

	public static MySqlDBManagerBak getInstance() {
		return InnerHolder.INSTANCE;
	}

	private MySqlDBManagerBak() {
		this.init(URL, USER, PSW);
	}

	private static class InnerHolder {
		static final MySqlDBManagerBak INSTANCE = new MySqlDBManagerBak();
	}

	public boolean init(final String url, final String user, final String psw) {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url, user, psw);
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
		return false;
	}

	public boolean execSQL(String sql) throws Exception {
		boolean execResult = false;
		Statement statement = null;
		try {
			statement = conn.createStatement();
			if (statement != null) {
				execResult = statement.execute(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			execResult = false;
		}
		statement.close();
		return execResult;
	}

}
