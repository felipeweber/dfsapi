package dfs.datanode.comm.controller.protocol;

/**
 * Declare the methods that will implement the datanode side protocol
 * for communication between datanode server and clients
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationReqProtocolHandler {

	public void startProtocol() throws Exception;
	
	public void ls() throws Exception;
	
}
