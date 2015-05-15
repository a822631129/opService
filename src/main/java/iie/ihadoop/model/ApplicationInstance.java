package iie.ihadoop.model;

/**
 * application实体，对应application表
 * 
 * @author sunyang
 *
 */
public class ApplicationInstance {
	//appid,otid,sequence,status,startTime

	public String appid; //算子任务起的application的id
	public String otid; //算子任务id
	public int squence; //一个算子任务产生application的次序
	public String status; //application状态
	public String startTime; //application产生的时间
	
	public ApplicationInstance() {
	}
	public ApplicationInstance(String appid, String otid, int squence,
			String status, String startTime) {
		this.appid = appid;
		this.otid = otid;
		this.squence = squence;
		this.status = status;
		this.startTime = startTime;
	}
	
	public String getAppid() {
		return appid;
	}
	public void setAppid(String appid) {
		this.appid = appid;
	}
	public String getOtid() {
		return otid;
	}
	public void setOtid(String otid) {
		this.otid = otid;
	}

	public int getSquence() {
		return squence;
	}
	public void setSquence(int squence) {
		this.squence = squence;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	
}
