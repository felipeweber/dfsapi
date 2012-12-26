package dfs.controller.fileoperations;

/**
 * Interface declaring the methods to coordinate a file operation.
 * The methods form this interface are called after the client has
 * 	made contact and requested a given operation.
 * It will perform the internal checks and send a request to the
 * 	proper datanode to respond to the client
 *
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperations {
	
	public void requestFileOperation(Long operationId) throws Exception;

	public void parseFinishedDatanodeWriteOperation(String filePath, String fileName, String datanodeId, String errorMessage)  throws Exception;

	public void parseFinishedDatanodeReadOperation(String filePath, String name, String datanodeId, String errorMessage, String lockAquireeId)  throws Exception;
	
}
