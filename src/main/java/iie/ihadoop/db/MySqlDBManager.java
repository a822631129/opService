package iie.ihadoop.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * tomcat数据库连接池管理类
 * 使用为tomcat部署环境
 */
public class MySqlDBManager {
	private static final Log log = LogFactory.getLog(MySqlDBManager.class);
	private static final String CONFIGFILE = "dbcp.properties";
	private static DataSource dataSource;

//	public static void main(String[] args) {
//		long begin = System.currentTimeMillis();
//		for (int i = 0; i < 100; i++) {
//			Connection conn = MySqlDBManager.getConn();
//			System.out.print(i + "   ");
//			MySqlDBManager.closeConn(conn);
//		}
//		long end = System.currentTimeMillis();
//		System.out.println("用时：" + (end - begin));
//	}

	static {
		Properties dbProperties = new Properties();
		try {
			dbProperties.load(MySqlDBManager.class.getClassLoader().getResourceAsStream(CONFIGFILE));
			
			dataSource = BasicDataSourceFactory.createDataSource(dbProperties);
			Connection conn = getConn();
			DatabaseMetaData mdm = conn.getMetaData();
			log.info("Connected to " + mdm.getDatabaseProductName() + " "
					+ mdm.getDatabaseProductVersion());
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			log.error("初始化连接池失败：" + e);
		}
	}

	private MySqlDBManager() {
	}

	/**
	 * 获取链接，用完后记得关闭
	 * 
	 * @see {@link MySqlDBManager#closeConn(Connection)}
	 * @return
	 */
	public static final Connection getConn() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			log.error("获取数据库连接失败：" + e);
		}
		return conn;
	}

	/**
	 * 关闭连接
	 * 
	 * @param conn
	 *            需要关闭的连接
	 */
	public static void closeConn(Connection conn) {
		try {
			if (conn != null && !conn.isClosed()) {
				conn.setAutoCommit(true);
				conn.close();
			}
		} catch (SQLException e) {
			log.error("关闭数据库连接失败：" + e);
		}
	}
	
	public static boolean execSQL(String sql) throws Exception {
		boolean execResult = false;
		Connection conn = getConn();
		Statement statement = null;
		try {		
			statement = conn.createStatement();
			if (statement != null) {
				execResult = statement.execute(sql);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		statement.close();
		closeConn(conn);
		return execResult;
	}
}