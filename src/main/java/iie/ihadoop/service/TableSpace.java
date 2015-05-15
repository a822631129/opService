package iie.ihadoop.service;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import iie.ihadoop.model.Columndesc;
import iie.ihadoop.util.JavaUtil;

@Path("/tablespace")
public class TableSpace {
	
	/**
	 * 查看hive表模式（即列的描述信息）
	 * @param dbname 数据库名称
	 * @param tblname 表名称
	 * @return 返回列描述信息
	 * @throws Exception 抛出异常
	 */
	@GET
	@Path("/{dbname}/{tblname}")
	public String getTable(@PathParam("dbname") String dbname, @PathParam("tblname") String tblname) throws Exception {
		List<Columndesc> ColumnDescs = getDesc(dbname,tblname); //获得列描述信息
		if (ColumnDescs.size() == 0) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the table is not exist")); //生成xml响应
		}
		String result = creatColumnDescsXML(ColumnDescs); //生成xml响应
		return result;
	}
	
	/**
	 * 删除hive表
	 * @param dbname 数据库名称
	 * @param tblname 表名称
	 * @return 返回信息
	 * @throws Exception
	 */
	@DELETE
	@Path("/{dbname}/{tblname}")
	public String dropTable(@PathParam("dbname") String dbname, @PathParam("tblname") String tblname) throws Exception {
		HiveConf hiveconf = new HiveConf();
		HiveMetaStoreClient hivemsclient = new HiveMetaStoreClient(hiveconf);
		boolean isTableExist = hivemsclient.tableExists(dbname, tblname);
		if (!isTableExist) {
			return JavaUtil.createXml1("error", JavaUtil.errMap("the table is not exist")); //生成xml响应
		}
		hivemsclient.dropTable(dbname, tblname); //删除表
		return JavaUtil.createXml1("response", JavaUtil.errMap("")); 
	}
	
    /**
     * 将列描述信息转换成XML格式
     * @param columndesclist 存储列描述信息的List
     * @return 返回xml格式列描述信息
     * @throws Exception 抛出异常
     */
	private String creatColumnDescsXML(List<Columndesc> columndesclist) throws Exception {
		Document document = DocumentHelper.createDocument();
		Element rootelement = document.addElement("response");
		Element errmsg = rootelement.addElement("errmsg");
		errmsg.setText("");
		Element columndescs = rootelement.addElement("columndescs");
		for (int i = 0; i < columndesclist.size(); i++) {
		    Element columndesc = columndescs.addElement("columndesc");
		    Element name = columndesc.addElement("name");
		    name.setText(columndesclist.get(i).getName());
		    Element type = columndesc.addElement("type");
		    type.setText(columndesclist.get(i).getType());
		    Element ispart = columndesc.addElement("ispart");
		    ispart.setText(columndesclist.get(i).getIspart());	    
		}
		return JavaUtil.formatXML(document);
	}
	
	/**
	 * 获取hive表的每个列描述信息，放到list中
	 * @param dbname 数据库名称
	 * @param tblname 表名称
	 * @return 返回列描述信息
	 * @throws Exception 抛出异常
	 */
	private List<Columndesc> getDesc(String dbname,String tblname) throws Exception {
		List<Columndesc> columndescs = new ArrayList<Columndesc>();
		HiveConf hiveconf = new HiveConf();
		HiveMetaStoreClient hivemsclient = new HiveMetaStoreClient(hiveconf);
		boolean isTableExist = hivemsclient.tableExists(dbname, tblname);
		if (!isTableExist) {
			return columndescs;
		}
		List<FieldSchema> fields = hivemsclient.getFields(dbname, tblname); //获取非分区列
		List<FieldSchema> schemas = hivemsclient.getSchema(dbname, tblname); //获得全部列（分区和非分区列）
		
		int fieldSize = fields.size();
		int schemaSize = schemas.size();
		for (int i = 0; i < fieldSize; i++) { //先组织非分区列的信息
			Columndesc columndesc = new Columndesc();
			columndesc.setName(schemas.get(i).getName());
			columndesc.setType(schemas.get(i).getType());
			columndesc.setIspart("FALSE");
			columndescs.add(columndesc);
		}
		for (int j = fieldSize; j < schemaSize; j++) { //再组织分区列的信息
			Columndesc columndesc = new Columndesc();
			columndesc.setName(schemas.get(j).getName());
			columndesc.setType(schemas.get(j).getType());
			columndesc.setIspart("TRUE");
			columndescs.add(columndesc);
		}
		hivemsclient.close();
		return columndescs;
	}

}
