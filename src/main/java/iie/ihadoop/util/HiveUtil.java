package iie.ihadoop.util;

import iie.ihadoop.db.HiveInstanceDAO;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCancelOperationReq;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusReq;
import org.apache.hive.service.cli.thrift.TGetOperationStatusResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import sun.misc.BASE64Decoder;

/**
 * hive工具类
 * @author sunyang
 *
 */
public class HiveUtil {
	
	private TOperationHandle stmtHandle = new TOperationHandle();
	private String resourceManagerIp = "";
	
	public HiveUtil() {
		
	}
	
	/**
	 * 为otid为参数的构造器，给属性赋值，获得执行hive算子的句柄
	 * @param otid 算子任务id
	 * @throws Exception
	 */
	public HiveUtil(String otid) throws Exception {
		HiveInstanceDAO hidao = new HiveInstanceDAO();
		String detail = hidao.getDetailByOtid(otid);
		byte[] stmt = new byte[256];
		BASE64Decoder dec = new BASE64Decoder();
		stmt = dec.decodeBuffer(detail);
		TDeserializer de = new TDeserializer();
		de.deserialize(this.stmtHandle, stmt);
		
		YarnUtil yarnUtil = new YarnUtil();
		String rmWebAppAddr = yarnUtil.getRmWebappAddr();
		this.resourceManagerIp = rmWebAppAddr.substring(0, rmWebAppAddr.lastIndexOf(":"));		
	}
	
	/**
	 * 获得hvie算子任务状态
	 * @return
	 * @throws Exception
	 */
	public String getStatus() throws Exception {
		TSocket transport = new TSocket(this.resourceManagerIp, 10000);
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(transport));
		transport.open();
		
		TGetOperationStatusReq req = new TGetOperationStatusReq();
		req.setOperationHandle(this.stmtHandle);
		TGetOperationStatusResp statusResp = client.GetOperationStatus(req);
		
		System.out.println("status===="+statusResp.getOperationState().name());
		/* the hive operation state:
		  INITIALIZED_STATE(0),
		  RUNNING_STATE(1),
		  FINISHED_STATE(2),
		  CANCELED_STATE(3),
		  CLOSED_STATE(4),
		  ERROR_STATE(5),
		  UKNOWN_STATE(6),
		  PENDING_STATE(7); */
		 
		String opState = statusResp.getOperationState().name();
		transport.close();
		return opState;
	}
	
	
	/**
	 * 用来取消正在执行的hive任务。
	 */
	public void killHiveOp() throws Exception {	
		TSocket transport = new TSocket(this.resourceManagerIp, 10000);
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(transport));
		transport.open();
		TOpenSessionReq openReq = new TOpenSessionReq();
		TOpenSessionResp openResp = client.OpenSession(openReq);
		openResp.getSessionHandle();
		TCancelOperationReq cancelReq = new TCancelOperationReq();
		cancelReq.setOperationHandle(this.stmtHandle);
		client.CancelOperation(cancelReq);

		// TCloseOperationReq closeReq = new TCloseOperationReq();
		// closeReq.setOperationHandle(stmtHandle);
		// client.CloseOperation(closeReq);
		// TCloseSessionReq closeConnectionReq = new
		// TCloseSessionReq(sessHandle);
		// client.CloseSession(closeConnectionReq);

		transport.close();
	}

	/**
	 * 关闭请求操作的相关东西
	 * @throws Exception
	 */
	public void closeOperationReq() throws Exception {
		TSocket transport = new TSocket(this.resourceManagerIp, 10000);
		TCLIService.Client client = new TCLIService.Client(new TBinaryProtocol(transport));
		transport.open();
		TOpenSessionReq openReq = new TOpenSessionReq();
		TOpenSessionResp openResp = client.OpenSession(openReq);
		TSessionHandle sessHandle = openResp.getSessionHandle();
		TCloseOperationReq closeReq = new TCloseOperationReq();
		closeReq.setOperationHandle(stmtHandle);
		client.CloseOperation(closeReq);
		TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
		client.CloseSession(closeConnectionReq);
		transport.close();
	}
	
	/**
	 * 检验是否存在数据库
	 * @param dbName 数据库名次
	 * @return
	 * @throws Exception
	 */
    public boolean existDB(String dbName) throws Exception {
    	HiveConf hiveConf = new HiveConf();
		HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
		List<String> databases = hiveMetaStoreClient.getAllDatabases();
		hiveMetaStoreClient.close();
		if(databases.contains(dbName)) {
			return true;
		} else {
			return false;
		}
	}
    
    /**
     * 创建数据库
     * @param dbName 数据库名称
     * @param localtionUri 数据库文件存储路径
     * @throws Exception
     */
    public void createDB(String dbName, String localtionUri) throws Exception {
    	HiveConf hiveConf = new HiveConf();
		HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
		Database database = new Database();
		database.setName(dbName);
		database.setLocationUri(localtionUri);
		hiveMetaStoreClient.createDatabase(database);
		hiveMetaStoreClient.close();
    }
    
    /**
     * 删除数据库
     * @param dbName 数据库名称
     * @throws Exception
     */
    public void dropDB(String dbName) throws Exception {
    	HiveConf hiveConf = new HiveConf();
		HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
		hiveMetaStoreClient.dropDatabase(dbName, true, true, true); //name,deleteData,ignoreUnknownDb,cascade
		hiveMetaStoreClient.close();
    }
}
