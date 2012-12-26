package dfs.datanode.comm.controller.protocol;

import dfs.common.configparser.DatanodeInfo;


/**
 * Protocol for communicating end of file operations to the Controller
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationResProtocol {

	public void write(int operationId, String errorMessage);
	
	public DatanodeInfo findNextDatanodeOnReplicationPipeline(int operationId);

	public void read(int operationId, String errorMessage);
	
}
