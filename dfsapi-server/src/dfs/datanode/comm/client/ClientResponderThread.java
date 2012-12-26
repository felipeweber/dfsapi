package dfs.datanode.comm.client;

import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerRequestProto;
import dfs.datanode.comm.client.protocol.FileOperationProtocol;
import dfs.datanode.comm.client.protocol.impl.FileOperationProtocolImpl;
import dfs.datanode.comm.controller.operation.FileOperationsRes;
import dfs.datanode.comm.controller.operation.impl.FileOperationsResImpl;

/**
 * A Thread that connects to the client to perform given operations
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ClientResponderThread extends Thread {

	private int operationType;
	private Object requestParameterProto;
	
	public ClientResponderThread(int operationType, Object requestParameterProto) {
		this.operationType = operationType;
		this.requestParameterProto = requestParameterProto;
	}
	
	@Override
	public void run() {
		String errorMessage = null;
		int operationId;
		FileOperationsRes fileOperationsRes;
		
		switch(this.operationType) {
		
		case OperationType.FILE_OP_WRITE:
			Logger.logDebug("Starting write request with client", this.getClass());
			try {
				FileOperationProtocol fileOpProtocol = new FileOperationProtocolImpl(); 
				fileOpProtocol.writeToFile((FileWriteControllerRequestProto) requestParameterProto);
			} catch (Exception e) {
				System.out.println("ERROR RECEIVING FILE FROM CLIENT");
				e.printStackTrace();
				// set error message
				errorMessage = e.getMessage();
				
			}
			operationId = ((FileWriteControllerRequestProto) requestParameterProto).getOperationId();
			Logger.logDebug("Ended write request with client", this.getClass());
			
			// send response to the controller
			fileOperationsRes = new FileOperationsResImpl();
			fileOperationsRes.sendWriteReponse(operationId, errorMessage);
			
			Logger.logDebug("Ended write request with client", this.getClass());
			break;
		
		case OperationType.FILE_OP_READ:
			Logger.logDebug("Starting read request with client", this.getClass());
			try {
				FileOperationProtocol fileOpProtocol = new FileOperationProtocolImpl(); 
				fileOpProtocol.readFromFile((FileReadControllerRequestProto) requestParameterProto);
			} catch (Exception e) {
				System.out.println("ERROR SENDING FILE TO CLIENT");
				e.printStackTrace();
				// set error message
				errorMessage = e.getMessage();
				
			}
			operationId = ((FileReadControllerRequestProto) requestParameterProto).getOperationId();
			
			// send response to the controller
			fileOperationsRes = new FileOperationsResImpl();
			fileOperationsRes.sendReadReponse(operationId, errorMessage);
			
			Logger.logDebug("Ended read request with client", this.getClass());
			break;
		}
	}
	
}
