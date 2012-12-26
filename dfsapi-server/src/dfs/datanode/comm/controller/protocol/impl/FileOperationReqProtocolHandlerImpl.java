package dfs.datanode.comm.controller.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerRequestProto;
import dfs.datanode.comm.client.ClientResponderThread;
import dfs.datanode.comm.client.operation.FileOperations;
import dfs.datanode.comm.client.operation.impl.FileOperationsImpl;
import dfs.datanode.comm.client.protocol.FileOperationProtocol;
import dfs.datanode.comm.client.protocol.impl.FileOperationProtocolImpl;
import dfs.datanode.comm.controller.protocol.FileOperationReqProtocolHandler;

/**
 * Implements the datanode side of the protocol of communication
 * between datanode and clients
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationReqProtocolHandlerImpl implements FileOperationReqProtocolHandler {

	DataInputStream in;
	DataOutputStream out;
	
	public FileOperationReqProtocolHandlerImpl(DataInputStream in, DataOutputStream out) {
		this.in = in;
		this.out = out;
	}
	
	public void startProtocol() throws Exception {
		
		// 1. waits for the controller to send an operation code
		int operationType = in.readInt();
		
		switch (operationType) {
		case OperationType.FILE_OP_LS:
			ls();
			break;
		case OperationType.FILE_OP_WRITE:
			write();
			break;
		case OperationType.FILE_OP_READ:
			read();
			break;
		}
	}
	
	public void write() throws Exception {
		Logger.logDebug("Starting write requisition from Controller protocol", this.getClass());
		// 1. Receive the operation details
		FileWriteControllerRequestProto fileWriteRequest = FileWriteControllerRequestProto.parseDelimitedFrom(in); 
		
		// Start Thread to connect to the client to receive the file
		ClientResponderThread clientResponderThread = new ClientResponderThread(OperationType.FILE_OP_WRITE, fileWriteRequest);
		clientResponderThread.start();
		
		Logger.logDebug("Ended write requisition from Controller protocol", this.getClass());
	}
	
	public void read() throws Exception {
		Logger.logDebug("Starting read requisition from Controller protocol", this.getClass());
		// 1. Receive the operation details
		FileReadControllerRequestProto fileReadRequest = FileReadControllerRequestProto.parseDelimitedFrom(in); 
		
		// Start Thread to connect to the client to receive the file
		ClientResponderThread clientResponderThread = new ClientResponderThread(OperationType.FILE_OP_READ, fileReadRequest);
		clientResponderThread.start();
		
		Logger.logDebug("Ended write requisition from Controller protocol", this.getClass());
	}
	
	/***** OLD STUFF *****/
	
	@Deprecated
	public void ls() throws Exception {
		// 1. Receive the operation details
		FileOperationLsRequestProto lsRequestProto; 
		lsRequestProto = FileOperationLsRequestProto.parseDelimitedFrom(in);
		
		// Perform the ls
		FileOperations fileOperations = new FileOperationsImpl();
		FileInfoLsProto fileInfoLs = fileOperations.performLs(lsRequestProto);
		
		// Respond to the client
		// Responds the FileInfoLs object to the client
		FileOperationProtocol resProtocol = new FileOperationProtocolImpl();
		resProtocol.respondLsRequest(lsRequestProto, fileInfoLs);
	}
	
	
}
