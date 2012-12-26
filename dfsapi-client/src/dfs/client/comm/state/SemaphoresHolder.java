package dfs.client.comm.state;

import java.util.HashMap;

/**
 * Holds a list of semaphores used by the client
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class SemaphoresHolder {

	/**
	 * List of semaphores identified by operation id
	 */
	private static HashMap<Long, DatanodeOperationSemaphore> semaphores = new HashMap<Long, DatanodeOperationSemaphore>();

	
	private static DatanodeOperationSemaphore operateSemaphore(int op, Long operationId, DatanodeOperationSemaphore datanodeOperationSemaphore) {
		// op
		//  1. get
		//  2. set
		//  3. remove
		synchronized (semaphores) {
			switch (op) {
			case 1:
				return semaphores.get(operationId);
			case 2:
				semaphores.put(operationId, datanodeOperationSemaphore);
				break;
			case 3:
				semaphores.remove(operationId);
				break;
			}
				
		}
		return null;
	}
	
	public static DatanodeOperationSemaphore getSemaphore(int operationId) {
		return operateSemaphore(1, new Long(operationId), null);
	}
	
	public static DatanodeOperationSemaphore addSempahore(int operationId, int operationType) {
		DatanodeOperationSemaphore semaphore = new DatanodeOperationSemaphore(operationType);
		operateSemaphore(2, new Long(operationId), semaphore);
		return semaphore;
	}
	
	public static DatanodeOperationSemaphore addSempahore(int operationId, int operationType, Object operationParameter) {
		DatanodeOperationSemaphore semaphore = new DatanodeOperationSemaphore(operationType, operationParameter);
		operateSemaphore(2, new Long(operationId), semaphore);
		return semaphore;
	}
	
	public static void removeSemaphore(int operationId) {
		operateSemaphore(3, new Long(operationId), null);
	}
	
}
