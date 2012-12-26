package dfs.datanode.comm.replication.send;

import java.math.BigInteger;
import java.util.HashMap;

import dfs.common.configparser.DatanodeInfo;
import dfs.datanode.comm.datanode.protocol.ReplicationPipelineProtocolImpl;
import dfs.datanode.comm.replication.ThreadPoolManager;

/**
 * Manages sending data blocks to the next datanode in the
 * replication pipeline
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ReplicationPipelineSender {

	// Map by operationId
	private HashMap<Long, DataBlockSend> dataBlocksSendMap = new HashMap<Long, DataBlockSend>();
	
	private static final ReplicationPipelineSender INSTANCE = new ReplicationPipelineSender();
	private ReplicationPipelineSender() { };
	public static ReplicationPipelineSender getInstance() {
		return INSTANCE;
	}
	
	/** STEP 1
	 * Initialize the replication pipeline and send a request to the Controller
	 * 	requiring information about the next datanode in the pipeline
	 */
	public void initializeReplicationPipeline(Long operationId, BigInteger globalFileKey, int totalFileSize) {
		synchronized (dataBlocksSendMap) {
			DataBlockSend dataBlockSend = new DataBlockSend(globalFileKey, totalFileSize, (int)operationId.longValue());
			dataBlocksSendMap.put(operationId, dataBlockSend);
		}
		// Send a request to the Controller to get the next datanode info.
		// This same thread will subsequently call startNextDatanode
		RequestNextDatanodeThread requestNextDatanodeThread = new RequestNextDatanodeThread((int) operationId.longValue());
		ThreadPoolManager threadPoolManager = ThreadPoolManager.getInstance();
		threadPoolManager.addThread(requestNextDatanodeThread);
	}
	
	/** STEP 2
	 * Set the nextDatanodeId, after receiving it from the Controller, for a given
	 * 	DataBlock operation and trigger a thread to start communicating with that datanode
	 */
	public void startNextDatanode(int operationId, DatanodeInfo nextDatanodeInfo) throws Exception {
		DataBlockSend dataBlockSend = null;
		synchronized (dataBlocksSendMap) {
			// if received a next datanode, set info and proceed
			if (nextDatanodeInfo != null && nextDatanodeInfo.getId() != null && !nextDatanodeInfo.getId().trim().isEmpty()) {
				dataBlockSend = dataBlocksSendMap.get(Long.valueOf(operationId));
				dataBlockSend.setNextDatanodeInfo(nextDatanodeInfo);
			} else {
				// if haven't received a next datanode, cancel operation
				dataBlocksSendMap.remove(Long.valueOf(operationId));
			}
		}
		
		// received a nextDatanode, proceed
		if (dataBlockSend != null) {
			// start protocol to effectively send the file
			ReplicationPipelineProtocolImpl replicationPipelineProtocolImpl = new ReplicationPipelineProtocolImpl(dataBlockSend);
			replicationPipelineProtocolImpl.writeFileNextDatanode();
			// operation finished, remove it
			synchronized (dataBlocksSendMap) {
				dataBlocksSendMap.remove(Long.valueOf(operationId));
			}
		}
		
	}
	
}