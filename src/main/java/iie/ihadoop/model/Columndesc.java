package iie.ihadoop.model;

/**
 * hive表的列描述实体
 * 
 * @author sunyang
 *
 */
public class Columndesc {
	private String name; //名称
	private String type; //类型
	private String ispart; //是否分区
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getIspart() {
		return ispart;
	}
	public void setIspart(String ispart) {
		this.ispart = ispart;
	}

	
}
