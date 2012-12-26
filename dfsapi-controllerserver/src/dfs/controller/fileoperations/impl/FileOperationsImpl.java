package dfs.controller.fileoperations.impl;

import java.util.List;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.controller.comm.datanode.protocol.FileOperationReqProtocol;
import dfs.controller.comm.datanode.protocol.impl.FileOperationReqProtocolImpl;
import dfs.controller.comm.state.CurrentOperations;
import dfs.controller.comm.state.Datanodes;
import dfs.controller.comm.state.Operation;
import dfs.controller.comm.state.parameters.FileReadOperationParameter;
import dfs.controller.comm.state.parameters.FileWriteOperationParameter;
import dfs.controller.fileoperations.FileOperations;
import dfs.controller.filestructure.FileStructure;
import dfs.controller.filestructure.operations.FileStructureOperation;
import dfs.controller.filestructure.operations.impl.FileStructureOperationImpl;

/**
 * Implements the file operations coordination in a simple way
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationsImpl implements FileOperations {

	/**
	 * Perform a given file operation based on the operationId,
	 * 	which will be used to retrieve the operation details from
	 * 	the CurrentOperations class	
	 */
	public void requestFileOperation(Long operationId) throws Exception {
		Operation operation = CurrentOperations.getOperation(operationId);
		
		switch(operation.getOperationType()) {
		case OperationType.FILE_OP_LS:
			ls(operation);
			break;
		case OperationType.FILE_OP_WRITE:
			writeFile(operation);
			break;
		case OperationType.FILE_OP_READ:
			readFile(operation);
			break;
		}	
	}
	
	private void writeFile(Operation operation) throws Exception {
		Logger.logDebug("Start operation to send writeFile request to datanode", getClass());
		// on a write request, the parameter is a FileWriteOperationParameter
		FileWriteOperationParameter fileWriteParm = (FileWriteOperationParameter) operation.getParamProto();
		
		// find all datanodes that will be receiving that file
		List<DatanodeInfo> datanodes = Datanodes.findDatanodesForFileWrite(fileWriteParm.getDfsFile());
		Logger.logDebug("Operation will send write request to " + datanodes.size() + " datanodes: ", getClass());
		// add the chosen datanodes to the operation parameter
		for (DatanodeInfo datanodeInfo : datanodes) {
			fileWriteParm.addDatanode(datanodeInfo);
			Logger.logDebug(">>> " + datanodeInfo, getClass());
		}
		
		// remove valid datanodes from the dfsFile
		FileStructure.getInstance().removeValidDatanodes(fileWriteParm.getFilePath(), fileWriteParm.getDfsFile().getName());
		
		// send the request to the first datanode
		FileOperationReqProtocol fileOperationRequest = new FileOperationReqProtocolImpl(fileWriteParm.findNextDatanode(), operation);
		fileOperationRequest.requestFileOperation();
	}
	
	/**
	 * Parse a finished write operation by a datanode
	 */
	public void parseFinishedDatanodeWriteOperation(String filePath, String fileName, String datanodeId, String errorMessage) throws Exception {
		
		// change the fileStructure according to the response
		FileStructureOperation fileStructureOp = new FileStructureOperationImpl();
		fileStructureOp.parseWriteOperationResponse(filePath, fileName, datanodeId, errorMessage);
		
	}
	
	private void readFile(Operation operation) throws Exception {
		Logger.logDebug("Start operation to send readFile request to datanode", getClass());
		// on a read request, the parameter is a FileReadOperationParameter
		FileReadOperationParameter fileReadParm = (FileReadOperationParameter) operation.getParamProto();
		
		// find the datanode that will be sending that file
		List<DatanodeInfo> datanodes = Datanodes.findDatanodesForFileWrite(fileReadParm.getDfsFile());
		Logger.logDebug("Operation will send read request to one of the valid datanodes: ", getClass());
		// add the chosen datanodes to the operation parameter
		for (DatanodeInfo datanodeInfo : datanodes) {
			fileReadParm.addDatanode(datanodeInfo);
			Logger.logDebug(">>> " + datanodeInfo, getClass());
		}
		
		// TODO receive file from multiple datanodes, someday
		
		// send the request to the first datanode
		FileOperationReqProtocol fileOperationRequest = new FileOperationReqProtocolImpl(fileReadParm.findNextDatanode(), operation);
		fileOperationRequest.requestFileOperation();
	}
	
	/**
	 * Parse a finished read operation by a datanode
	 */
	public void parseFinishedDatanodeReadOperation(String filePath, String fileName, String datanodeId, String errorMessage, String lockAquireeId)  throws Exception {
		// change the fileStructure according to the response
		FileStructureOperation fileStructureOp = new FileStructureOperationImpl();
		fileStructureOp.parseReadOperationResponse(filePath, fileName, datanodeId, errorMessage, lockAquireeId);
	}
	
	/************* OLD STUFF ***********/
	
	@Deprecated
	private void ls(Operation operation) throws Exception {
		// on an ls request, the parameter is a FilePathProto
		FilePathProto path = (FilePathProto) operation.getParamProto();
		
		// get the possible datanodes ids we can ls from
		FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
		List<String> validDatanodesIds = fileStructureOperation.ls(path.getPath());
		
		// figure the preferred datanode
		//DatanodeInfo datanode = Datanodes.findPreferredDatanode(validDatanodesIds);
		//DatanodeInfo datanode = Datanodes.findPreferredDatanode(null);
		DatanodeInfo datanode = new DatanodeInfo("datanode1", "127.0.0.1", 30000);
		
		// send the request to the datanode
		FileOperationReqProtocol fileOperationRequest = new FileOperationReqProtocolImpl(datanode, operation);
		fileOperationRequest.requestFileOperation();
		
		
	}
}
