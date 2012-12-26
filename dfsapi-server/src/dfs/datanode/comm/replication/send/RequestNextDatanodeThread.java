package dfs.datanode.comm.replication.send;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.datanode.comm.controller.protocol.FileOperationResProtocol;
import dfs.datanode.comm.controller.protocol.impl.FileOperationResProtocolImpl;

/**
 * Send a request to the Controller to get the nextDatanodeInfo for the Replication Pipeline
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class RequestNextDatanodeThread extends Thread {

	private int operationId;
	
	public RequestNextDatanodeThread(int operationId) {
		this.operationId = operationId;
	}
	
	@Override
	public void run() {
		// 1. Send a request to the Controller to find the next datanode on the Replication Pipeline
		FileOperationResProtocol fileOperationResProtocol = new FileOperationResProtocolImpl();
		DatanodeInfo nextDatanodeInfo = fileOperationResProtocol.findNextDatanodeOnReplicationPipeline(operationId);
		
		// set the nextDatanodeId on the ReplicationPipelineSender
		//	this will also trigger a task to start communicating with that datanode
		ReplicationPipelineSender replicationPipelineSender = ReplicationPipelineSender.getInstance();
		try {
			replicationPipelineSender.startNextDatanode(operationId, nextDatanodeInfo);
		} catch (Exception e) {
			Logger.logError("ERROR ON THE REPLICATION PIPELINE WITH DATANODE " + nextDatanodeInfo.getId(), e, getClass());
			e.printStackTrace();
		}
		// TODO treat error
	}
	
}
