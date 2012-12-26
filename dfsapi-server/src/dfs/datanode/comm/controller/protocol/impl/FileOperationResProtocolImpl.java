package dfs.datanode.comm.controller.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileReadDatanodeResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteDatanodeResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.OperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineNextDatanodeRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineNextDatanodeResponseProto;
import dfs.datanode.DatanodeConfigurations;
import dfs.datanode.comm.controller.ControllerConnection;
import dfs.datanode.comm.controller.protocol.FileOperationResProtocol;

/**
 * Protocol for communicating regarding File Operations with the Controller
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationResProtocolImpl implements FileOperationResProtocol {

	public DatanodeInfo findNextDatanodeOnReplicationPipeline(int operationId) {
		DatanodeInfo datanodeInfo = null;
		try {
			Logger.logDebug("Started sending findNextDatanodeOnReplicationPipeline to controller", getClass());
			ControllerConnection conn = new ControllerConnection(InternalConnectionType.CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE);
			
			if (!conn.connectOrReconnectToControllerServer()) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				Logger.logError("ERROR COMMUNICATING findNextDatanodeOnReplicationPipeline: Cannot connect to controller server", getClass());
			}
			
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			DataInputStream in = new DataInputStream(conn.getInputStream());
			
			// Parameter to the Controller: this datanodeId
			ReplicationPipelineNextDatanodeRequestProto.Builder nextDatanodeRequest = ReplicationPipelineNextDatanodeRequestProto.newBuilder();
			nextDatanodeRequest.setDatanodeId(DatanodeConfigurations.ID);
			
			OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
			operationIdentifierProto.setId(operationId);
			operationIdentifierProto.setOperationResponseType(OperationType.FILE_OP_REQ_NEXT_DATANODE);
			
			// 1. Send the operation identifier
			operationIdentifierProto.build().writeDelimitedTo(out);
			
			// 2. Send this datanodeId
			nextDatanodeRequest.build().writeDelimitedTo(out);
			
			// 3. Receive the nextDatanode Info
			ReplicationPipelineNextDatanodeResponseProto nextDatanodeResponse = ReplicationPipelineNextDatanodeResponseProto.parseDelimitedFrom(in);
			
			// build DatanodeInfo if received anything
			if (nextDatanodeResponse.getNextDatanodeId() != null) {
				datanodeInfo = new DatanodeInfo(nextDatanodeResponse.getNextDatanodeId(), nextDatanodeResponse.getHost(), nextDatanodeResponse.getStartPort());
				Logger.logDebug("Ended sending findNextDatanodeOnReplicationPipeline to controller. Received: " + datanodeInfo, getClass());
			} else {
				Logger.logDebug("Ended sending findNextDatanodeOnReplicationPipeline to controller. Received: NULL", getClass());
			}
			
			out.close();
			in.close();
			conn.close();
		} catch (Exception e) {
			Logger.logError("Could not sendfindNextDatanodeOnReplicationPipeline to Controller, " + e.getMessage(), getClass());
			e.printStackTrace();
		}
		
		return datanodeInfo;
		
	}
	
	public void write(int operationId, String errorMessage) {
		try {
			Logger.logDebug("Started sending write response to controller", getClass());
			ControllerConnection conn = new ControllerConnection(InternalConnectionType.CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE);
			
			if (!conn.connectOrReconnectToControllerServer()) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("ERROR COMMUNICATING END OF WRITE OPERATION: Cannot connect to controller server");
			}
			
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			DataInputStream in = new DataInputStream(conn.getInputStream());
			
			// Response that will be sent to the controller after thread finishes running
			FileWriteDatanodeResponseProto.Builder responseToController = FileWriteDatanodeResponseProto.newBuilder();
			
			OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
			operationIdentifierProto.setId(operationId);
			operationIdentifierProto.setOperationResponseType(OperationType.FILE_OP_WRITE);
			
			if (errorMessage != null)
				responseToController.setErrorMessage(errorMessage);
			
			responseToController.setDatanodeId(DatanodeConfigurations.ID);
			
			// 1. send the operation identifier
			operationIdentifierProto.build().writeDelimitedTo(out);
			
			// 2. send the response
			responseToController.build().writeDelimitedTo(out);
			
			out.close();
			in.close();
			conn.close();
			Logger.logDebug("Ended sending write response to controller", getClass());
		} catch (Exception e) {
			Logger.logError("Could not send write response to Controller, " + e.getMessage(), getClass());
			e.printStackTrace();
		}
	}
	
	
	public void read(int operationId, String errorMessage) {
		try {
			Logger.logDebug("Started sending read response to controller", getClass());
			ControllerConnection conn = new ControllerConnection(InternalConnectionType.CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE);
			
			if (!conn.connectOrReconnectToControllerServer()) {
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.println("ERROR COMMUNICATING END OF READ OPERATION: Cannot connect to controller server");
			}
			
			DataOutputStream out = new DataOutputStream(conn.getOutputStream());
			DataInputStream in = new DataInputStream(conn.getInputStream());
			
			// Response that will be sent to the controller after thread finishes running
			FileReadDatanodeResponseProto.Builder responseToController = FileReadDatanodeResponseProto.newBuilder();
			
			OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
			operationIdentifierProto.setId(operationId);
			operationIdentifierProto.setOperationResponseType(OperationType.FILE_OP_READ);
			
			if (errorMessage != null)
				responseToController.setErrorMessage(errorMessage);
			
			responseToController.setDatanodeId(DatanodeConfigurations.ID);
			
			// 1. send the operation identifier
			operationIdentifierProto.build().writeDelimitedTo(out);
			
			// 2. send the response
			responseToController.build().writeDelimitedTo(out);
			
			out.close();
			in.close();
			conn.close();
			Logger.logDebug("Ended sending read response to controller", getClass());
		} catch (Exception e) {
			Logger.logError("Could not send read response to Controller, " + e.getMessage(), getClass());
			e.printStackTrace();
		}
	}
}
