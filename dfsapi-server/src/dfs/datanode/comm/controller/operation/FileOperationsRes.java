package dfs.datanode.comm.controller.operation;

/**
 * Send responses to the Controller after finished file operations
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationsRes {

	/**
	 * Response to be sent after finishing a write operation from a client
	 */
	public void sendWriteReponse(int operationId, String errorMessage);

	public void sendReadReponse(int operationId, String errorMessage);
	
}
