package dfs.datanode.comm.replication.send;

import dfs.datanode.comm.datanode.protocol.ReplicationPipelineProtocolImpl;

/**
 * Send a request to the nextDatanode to start a block write
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class StartWriteNextDatanodeThread extends Thread {

	private int operationId;
	private String fileGlobalKey;
	private int fileSize;
	private ReplicationPipelineProtocolImpl replicationPipelineProtocolImpl;
	
	public StartWriteNextDatanodeThread(int operationId, String fileGlobalKey,
			int fileSize, ReplicationPipelineProtocolImpl replicationPipelineProtocolImpl) {
		super();
		this.operationId = operationId;
		this.fileGlobalKey = fileGlobalKey;
		this.fileSize = fileSize;
		this.replicationPipelineProtocolImpl = replicationPipelineProtocolImpl;
	}


	@Override
	public void run() {
		
	}
	
}
