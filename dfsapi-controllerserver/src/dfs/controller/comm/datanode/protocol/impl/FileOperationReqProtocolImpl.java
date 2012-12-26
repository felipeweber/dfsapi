package dfs.controller.comm.datanode.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;

import com.google.protobuf.ByteString;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerRequestProto;
import dfs.controller.comm.datanode.DatanodeConnection;
import dfs.controller.comm.datanode.protocol.FileOperationReqProtocol;
import dfs.controller.comm.state.Operation;
import dfs.controller.comm.state.parameters.FileReadOperationParameter;
import dfs.controller.comm.state.parameters.FileWriteOperationParameter;
import dfs.controller.filestructure.DFSFile;

public class FileOperationReqProtocolImpl implements FileOperationReqProtocol {

	private DatanodeInfo datanodeInfo;
	private Operation operation;
	private DatanodeConnection conn;
	private DataOutputStream out;
	private DataInputStream in;
	
	public FileOperationReqProtocolImpl(DatanodeInfo datanodeInfo, Operation operation) {
		this.datanodeInfo = datanodeInfo;
		this.operation = operation;
	}
	
	public void requestFileOperation() throws Exception {
		try {
			Logger.logDebug("Started protocol to send file operation request to datanode " + datanodeInfo.getId(), getClass());
			// connect to the datanode
			conn = new DatanodeConnection(datanodeInfo, operation.getOperationType());
			if (!conn.connectOrReconnect()) {
				Logger.logError("ERROR: Cannot connect to the datanode " + datanodeInfo.toString(), getClass());
				// TODO couldn't connect to the datanode, handle error by reverting stuff and trying again instead of throwing an exception, which is not catched anyway because we are in a THREAD!
				throw new Exception("ERROR: Cannot connect to the datanode " + datanodeInfo.toString());
			}

			out = new DataOutputStream(conn.getOutputStream());
			in = new DataInputStream(conn.getInputStream());
			Logger.logDebug("Connected and got streams to datanode " + datanodeInfo.getId(), getClass());
			
			switch(operation.getOperationType()) {
			case OperationType.FILE_OP_LS:
				ls();
				break;
			case OperationType.FILE_OP_WRITE:
				write();
				break;
			case OperationType.FILE_OP_READ:
				read();
				break;
			default:
				throw new Exception("ERROR: Unsupported operation: " + operation.getOperationType());
			}
		} catch (Exception e) {
			Logger.logError("ERROR REQUESTING FILE OPERATION TO A DATANODE", e, getClass());
			throw e;
		} finally {
			out.close();
			in.close();
			conn.close();
		}
		
		
	}

	private void write() throws Exception {
		Logger.logDebug("Started write protocol to datanode " + datanodeInfo.getId(), getClass());
		// Build request message
		FileWriteControllerRequestProto.Builder writeRequestProto = FileWriteControllerRequestProto.newBuilder();
		writeRequestProto.setClientId(operation.getClientIdentifier().getId());
		writeRequestProto.setClientHost(operation.getClientIdentifier().getHost());
		writeRequestProto.setClientPort(operation.getClientIdentifier().getListenerPort());
		writeRequestProto.setOperationId(operation.getId().intValue());
		
		// parameter: FileWriteOperationParameter
		FileWriteOperationParameter fileWriteParm = (FileWriteOperationParameter) operation.getParamProto();
		DFSFile dfsFile = fileWriteParm.getDfsFile();
		BigInteger fileKey = dfsFile.getFileKey();
		writeRequestProto.setFilekey(ByteString.copyFrom(fileKey.toByteArray()));
		writeRequestProto.setTotalFileSize(fileWriteParm.getTotalWriteSize());
		
		Logger.logDebug("Parameters built to send write request to datanode " + datanodeInfo.getId(), getClass());
		
		// 1. Send the operation type
		out.writeInt(operation.getOperationType());
		
		Logger.logDebug("Sent operation type to datanode " + datanodeInfo.getId(), getClass());
		
		// 2. Send the operation request to the datanode
		writeRequestProto.build().writeDelimitedTo(out);
		Logger.logDebug("Sent operation request to datanode " + datanodeInfo.getId(), getClass());
		
	}

	private void read() throws Exception {
		Logger.logDebug("Started read protocol to datanode " + datanodeInfo.getId(), getClass());
		// Build request message
		FileReadControllerRequestProto.Builder readRequestProto = FileReadControllerRequestProto.newBuilder();
		readRequestProto.setClientId(operation.getClientIdentifier().getId());
		readRequestProto.setClientHost(operation.getClientIdentifier().getHost());
		readRequestProto.setClientPort(operation.getClientIdentifier().getListenerPort());
		readRequestProto.setOperationId(operation.getId().intValue());
		
		// parameter: FileReadOperationParameter
		FileReadOperationParameter fileReadParm = (FileReadOperationParameter) operation.getParamProto();
		DFSFile dfsFile = fileReadParm.getDfsFile();
		BigInteger fileKey = dfsFile.getFileKey();
		readRequestProto.setFilekey(ByteString.copyFrom(fileKey.toByteArray()));
		readRequestProto.setOffset(fileReadParm.getOffset());
		readRequestProto.setSize(fileReadParm.getSize());

		Logger.logDebug("Parameters built to send read request to datanode " + datanodeInfo.getId(), getClass());
		
		// 1. Send the operation type
		out.writeInt(operation.getOperationType());
		out.flush();
		Logger.logDebug("Sent operation type to datanode " + datanodeInfo.getId(), getClass());
		
		// 2. Send the operation request to the datanode
		readRequestProto.build().writeDelimitedTo(out);
		Logger.logDebug("Sent operation request to datanode " + datanodeInfo.getId() + ", file global key: " + readRequestProto.getFilekey(), getClass());
		
	}
	
	
	@Deprecated
	private void ls() throws Exception {
		// Build request message
		FileOperationLsRequestProto.Builder lsRequestProto = FileOperationLsRequestProto.newBuilder();
		lsRequestProto.setClientId(operation.getClientIdentifier().getId());
		lsRequestProto.setClientHost(operation.getClientIdentifier().getHost());
		lsRequestProto.setClientPort(operation.getClientIdentifier().getListenerPort());
		lsRequestProto.setOperationId(operation.getId().intValue());
		lsRequestProto.setOperationType(operation.getOperationType());
		lsRequestProto.setPath((FilePathProto) operation.getParamProto());
		
		// 1. Send the operation code
		out.writeInt(operation.getOperationType());
		
		// 2. Send the operation request to the datanode
		lsRequestProto.build().writeDelimitedTo(out);
		
		
	}
	
}
