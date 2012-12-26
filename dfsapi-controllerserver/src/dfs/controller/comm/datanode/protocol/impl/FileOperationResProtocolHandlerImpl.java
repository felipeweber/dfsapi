package dfs.controller.comm.datanode.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileReadDatanodeResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteDatanodeResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.OperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineNextDatanodeRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineNextDatanodeResponseProto;
import dfs.controller.comm.datanode.protocol.FileOperationResProtocolHandler;
import dfs.controller.comm.state.CurrentOperations;
import dfs.controller.comm.state.Operation;
import dfs.controller.comm.state.parameters.FileReadOperationParameter;
import dfs.controller.comm.state.parameters.FileWriteOperationParameter;
import dfs.controller.fileoperations.FileOperations;
import dfs.controller.fileoperations.impl.FileOperationsImpl;

/**
 * Implements the controller server side of the protocol of communication
 * between controller and a datanode that's responding to a file operation
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationResProtocolHandlerImpl implements FileOperationResProtocolHandler {

	private DataInputStream in;
	private DataOutputStream out;
	
	public FileOperationResProtocolHandlerImpl(DataInputStream in, DataOutputStream out) {
		this.in = in;
		this.out = out;
	}
	
	public void startProtocol() throws Exception {
		
		// 1. waits for the datanode to send the operation he's responding to
		OperationIdentifierProto operationIdentifierProto = OperationIdentifierProto.parseDelimitedFrom(in);
		// find the internal operation
		Operation operation = CurrentOperations.getOperation(Long.valueOf((long)operationIdentifierProto.getId()));
		
		switch (operationIdentifierProto.getOperationResponseType()) {
		case OperationType.FILE_OP_WRITE:
			writeProtocol(operation);
			break;
		case OperationType.FILE_OP_REQ_NEXT_DATANODE:
			findNextDatanodeProtocol(operation);
			break;
		case OperationType.FILE_OP_READ:
			readProtocol(operation);
			break;
		}
	}
	

	private void writeProtocol(Operation operation) throws Exception {
		// 1. Receive the Write operation response
		FileWriteDatanodeResponseProto fileWriteResProto = FileWriteDatanodeResponseProto.parseDelimitedFrom(in);
		Logger.logDebug("Received a write completed response from datanode: " + fileWriteResProto.getDatanodeId(), getClass());
		
		// Get the internal parameter for a write operation, which is FileWriteOperationParameter
		FileWriteOperationParameter fileWriteParm = (FileWriteOperationParameter) operation.getParamProto();
		
		// Parse the finished operation
		FileOperations fileOperations = new FileOperationsImpl();
		fileOperations.parseFinishedDatanodeWriteOperation(fileWriteParm.getFilePath(), fileWriteParm.getDfsFile().getName(), fileWriteResProto.getDatanodeId(), fileWriteResProto.getErrorMessage());
		
	}
	
	private void findNextDatanodeProtocol(Operation operation) throws Exception {
		// 1. Receive the nextDatanodeRequest (will contain the datanodeId of the requestor)
		ReplicationPipelineNextDatanodeRequestProto nextDatanodeRequest = ReplicationPipelineNextDatanodeRequestProto.parseDelimitedFrom(in);
		Logger.logDebug("Received findNextDatanodeProtocol request from datanode " + nextDatanodeRequest.getDatanodeId(), getClass());
		
		// Get the internal parameter for a write operation, which is FileWriteOperationParameter
		FileWriteOperationParameter fileWriteParm = (FileWriteOperationParameter) operation.getParamProto();
		
		// Find the next preferred datanode
		DatanodeInfo datanodeInfo = fileWriteParm.findNextDatanode();
		
		// build response
		ReplicationPipelineNextDatanodeResponseProto.Builder nextDatanodeReponse = ReplicationPipelineNextDatanodeResponseProto.newBuilder();
		if (datanodeInfo != null) {
			nextDatanodeReponse.setNextDatanodeId(datanodeInfo.getId());
			nextDatanodeReponse.setHost(datanodeInfo.getHost());
			nextDatanodeReponse.setStartPort(datanodeInfo.getStartPort());
		}
		
		Logger.logDebug("Response of findNextDatanodeProtocol to datanode " +  nextDatanodeRequest.getDatanodeId() + " is: " + datanodeInfo, getClass());
		// 2. Send the response to the datanode
		nextDatanodeReponse.build().writeDelimitedTo(out);
	}

	private void readProtocol(Operation operation) throws Exception {
		// 1. Receive the Read operation response
		FileReadDatanodeResponseProto fileReadResProto = FileReadDatanodeResponseProto.parseDelimitedFrom(in);

		Logger.logDebug("Received a read completed response from datanode: " + fileReadResProto.getDatanodeId(), getClass());
		
		// Get the internal parameter for a read operation, which is FileReadOperationParameter
		FileReadOperationParameter fileReadParm = (FileReadOperationParameter) operation.getParamProto();
		
		// Parse the finished operation
		FileOperations fileOperations = new FileOperationsImpl();
		fileOperations.parseFinishedDatanodeReadOperation(fileReadParm.getFilePath(), fileReadParm.getDfsFile().getName(), fileReadResProto.getDatanodeId(), fileReadResProto.getErrorMessage(), String.valueOf(operation.getClientIdentifier().getId()));
		
	}
	
}
