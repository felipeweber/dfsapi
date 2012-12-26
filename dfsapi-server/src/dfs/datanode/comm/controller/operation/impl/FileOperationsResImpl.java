package dfs.datanode.comm.controller.operation.impl;

import dfs.datanode.comm.controller.operation.FileOperationsRes;
import dfs.datanode.comm.controller.protocol.FileOperationResProtocol;
import dfs.datanode.comm.controller.protocol.impl.FileOperationResProtocolImpl;

/**
 * Send responses to the Controller regarding file operations
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationsResImpl implements FileOperationsRes {

	/**
	 * Asks the Controller for a next datanode to send a data block currently being
	 * 	being received to (Replication Pipeline)
	 */
	public void findNextDatanodeOnReplicationPipeline(int operationId) {
		FileOperationResProtocol fileOperationResProtocol = new FileOperationResProtocolImpl();
		fileOperationResProtocol.findNextDatanodeOnReplicationPipeline(operationId);
	}
	
	/**
	 * Respond that a write request was finished, successfully or not
	 */
	public void sendWriteReponse(int operationId, String errorMessage) {
		FileOperationResProtocol fileOperationResProtocol = new FileOperationResProtocolImpl();
		fileOperationResProtocol.write(operationId, errorMessage);
	}
	
	/**
	 * Respond that a read request was finished, successfully or not
	 */
	public void sendReadReponse(int operationId, String errorMessage) {
		FileOperationResProtocol fileOperationResProtocol = new FileOperationResProtocolImpl();
		fileOperationResProtocol.read(operationId, errorMessage);
	}
	
}
