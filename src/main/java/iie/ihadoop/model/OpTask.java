package iie.ihadoop.model;

/**
 * 算子任务实体，对应optask表
 * 
 * @author sunyang
 *
 */
public class OpTask {
	
	public String otid; //算子任务id
	public String opid; //算子id
	public String processId; //场景id
	public String jobInstanceId; //场景实例id
	public String userName; //提交算子任务的用户
	public String queueName; //提交到的队列
	public String submitTime; //提交时间
	public String submission; //提交方式（同步/异步）
	public String status; //算子任务状态
	
	public OpTask() {}
	
	public OpTask(String otid, String opid, String processId,
			String jobInstanceId, String userName, String queueName,
			String submitTime, String submission, String status) {
		super();
		this.otid = otid;
		this.opid = opid;
		this.processId = processId;
		this.jobInstanceId = jobInstanceId;
		this.userName = userName;
		this.queueName = queueName;
		this.submitTime = submitTime;
		this.submission = submission;
		this.status = status;
	}
	public String getSubmission() {
		return submission;
	}
	public void setSubmission(String submission) {
		this.submission = submission;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getOtid() {
		return otid;
	}
	public void setOtid(String otid) {
		this.otid = otid;
	}
	public String getOpid() {
		return opid;
	}
	public void setOpid(String opid) {
		this.opid = opid;
	}
	public String getProcessId() {
		return processId;
	}
	public void setProcessId(String processId) {
		this.processId = processId;
	}
	public String getJobInstanceId() {
		return jobInstanceId;
	}
	public void setJobInstanceId(String jobInstanceId) {
		this.jobInstanceId = jobInstanceId;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getQueueName() {
		return queueName;
	}
	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
	public String getSubmitTime() {
		return submitTime;
	}
	public void setSubmitTime(String submitTime) {
		this.submitTime = submitTime;
	}
	
}
