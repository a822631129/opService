package iie.ihadoop.model;

/**
 * 对应业务表：hiveinstance
 * 
 * @author mashaolong
 * 
 */
public class HiveInstance {

	public String queryid;// hive中的queryid，目前是通过分析日志获得
	public String type;// hive-sql的语句类型，由hive定义 ，包括：
						// EXECUTE_STATEMENT，GET_TYPE_INFO，GET_CATALOGS，GET_SCHEMAS，GET_TABLES，GET_TABLE_TYPES，GET_COLUMNS，GET_FUNCTIONS
						// ，UNKNOWN

	public String otid;// 每次提交hive算子后由REST服务生成的算子唯一标识。
	public String submittime;// hive算子提交时间，值为JDBC中得到的时间点
	public String detail;// 每次提交hive请求的句柄，经过了编码

	public String getQueryid() {
		return queryid;
	}

	public void setQueryid(String queryid) {
		this.queryid = queryid;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getOtid() {
		return otid;
	}

	public void setOtid(String otid) {
		this.otid = otid;
	}

	public String getSubmittime() {
		return submittime;
	}

	public void setSubmittime(String submittime) {
		this.submittime = submittime;
	}

	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
	}

}
