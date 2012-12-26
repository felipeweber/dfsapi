package dfs.client.comm.datanode.protocol;


/**
 * Declare the methods that will implement the controller server side protocol
 * for communication between controller server and clients
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationDatanodeProtocolHandler {

	public void startProtocol() throws Exception;
	
}
