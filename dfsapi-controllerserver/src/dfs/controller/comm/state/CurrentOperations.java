package dfs.controller.comm.state;

import java.util.HashMap;

import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.ClientIdentifierProto;

public class CurrentOperations {

	/**
	 * Contains the operations: operationId / Operation
	 */
	private static HashMap<Long, Operation> operations = new HashMap<Long, Operation>();
	private static Long maxId = new Long(0);
	
	public static Long addOperation(ClientIdentifierProto clientIdentifier, int operationType, Object paramProto) {
		synchronized(operations) {
			Long id = maxId++;
			Operation operation = new Operation(id, clientIdentifier, operationType, paramProto);
			operations.put(id, operation);
			
			Logger.logInfo("Created a new operation: " + operation);
			
			return id;
		}
	}
	
	public static Operation getOperation(Long operationId) {
		synchronized(operations) {
			return operations.get(operationId);
		}
	}
	
}
