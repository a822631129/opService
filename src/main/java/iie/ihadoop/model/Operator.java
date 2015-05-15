package iie.ihadoop.model;

/**
 * 算子实体，对应operator表
 * 
 * @author sunyang
 *
 */
public class Operator {

	public String opid; //算子id
	public String version; //版本
	public String biztype; //业务类型
	public String provider; //开发商
	public String enginename; //依赖框架
	public String devlang; //依赖语言
	public String opjarpath; //算子zip包在hdfs路径
	public String descriptor; //描述信息
	public String instruction; //使用说明
	public String registerTime; //注册时间

	public Operator(String opid, String version, String biztype, String provider,
			String enginename, String devlang, String opjarpath, String libjar,
			String descriptor, String instruction, String registerTime) {
		super();
		this.opid = opid;
		this.version = version;
		this.biztype = biztype;
		this.provider = provider;
		this.enginename = enginename;
		this.devlang = devlang;
		this.opjarpath = opjarpath;
		this.descriptor = descriptor;
		this.instruction = instruction;
		this.registerTime = registerTime;
	}

	public String getBiztype() {
		return biztype;
	}

	public void setBiztype(String biztype) {
		this.biztype = biztype;
	}

	public String getEnginename() {
		return enginename;
	}

	public void setEnginename(String enginename) {
		this.enginename = enginename;
	}

	public String getOpid() {
		return opid;
	}

	public void setOpid(String opid) {
		this.opid = opid;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getProvider() {
		return provider;
	}

	public void setProvider(String provider) {
		this.provider = provider;
	}

	public String getDevlang() {
		return devlang;
	}

	public void setDevlang(String devlang) {
		this.devlang = devlang;
	}

	public String getOpjarpath() {
		return opjarpath;
	}

	public void setOpjarpath(String opjarpath) {
		this.opjarpath = opjarpath;
	}

	public String getDescriptor() {
		return descriptor;
	}

	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
	}

	public String getInstruction() {
		return instruction;
	}

	public void setInstruction(String instruction) {
		this.instruction = instruction;
	}

	public String getRegisterTime() {
		return registerTime;
	}

	public void setRegisterTime(String registerTime) {
		this.registerTime = registerTime;
	}

	public Operator() {
	}	

}
