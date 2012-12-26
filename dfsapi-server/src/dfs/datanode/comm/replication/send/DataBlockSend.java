package dfs.datanode.comm.replication.send;

import java.math.BigInteger;

import dfs.common.configparser.DatanodeInfo;
import dfs.datanode.comm.datanode.protocol.ReplicationPipelineProtocolImpl;

public class DataBlockSend {

	private int operationId;
	private BigInteger globalFileKey;
	private DatanodeInfo nextDatanodeInfo;
	private int totalFileSize;
	private int bytesSent;
	
	public DataBlockSend(BigInteger globalFileKey, int totalFileSize, int operationId) {
		this.globalFileKey = globalFileKey;
		this.totalFileSize = totalFileSize;
		this.operationId = operationId;
	}
	
	public BigInteger getGlobalFileKey() {
		return globalFileKey;
	}
	public void setGlobalFileKey(BigInteger globalFileKey) {
		this.globalFileKey = globalFileKey;
	}
	public int getBytesSent() {
		return bytesSent;
	}
	public void setBytesSent(int bytesSent) {
		this.bytesSent = bytesSent;
	}
	public DatanodeInfo getNextDatanodeInfo() {
		return nextDatanodeInfo;
	}

	public void setNextDatanodeInfo(DatanodeInfo nextDatanodeInfo) {
		this.nextDatanodeInfo = nextDatanodeInfo;
	}

	public int getTotalFileSize() {
		return totalFileSize;
	}
	public void setTotalFileSize(int totalFileSize) {
		this.totalFileSize = totalFileSize;
	}

	public int getOperationId() {
		return operationId;
	}

	public void setOperationId(int operationId) {
		this.operationId = operationId;
	}
	
	
}
