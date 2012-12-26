package dfs.controller.comm.datanode.protocol;


/**
 * Declare the methods that will implement the controller server side protocol
 * for communication between controller server and a datanode that's responding
 * to a file operation
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationResProtocolHandler {

	public void startProtocol() throws Exception;
	
}
