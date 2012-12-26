package dfs.client.comm.fileoperations;

import dfs.client.comm.controller.protocol.FileOperationReqProtocol;
import dfs.client.comm.controller.protocol.impl.FileOperationReqProtocolImpl;
import dfs.client.comm.state.DatanodeOperationSemaphore;
import dfs.client.comm.state.SemaphoresHolder;
import dfs.client.support.FileContent;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.OperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchControllerResponseProto;

/**
 * Connect to the datanode(s) and call the needed methods
 * to perform a given file operation 
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationsComm {

	/**
	 * Request a mkdir operation to the Controller
	 * 1. Send the request to the Controller
	 * 2. Wait for the response from the Controller
	 * @param build
	 * @return
	 */
	public static MkDirControllerResponseProto mkDir(MkDirClientRequestProto mkDirRequest) {
		try {
			// Requests the mkdir operation
			FileOperationReqProtocol controllerProtocolHandler = new FileOperationReqProtocolImpl();
			MkDirControllerResponseProto mkDirResponse = controllerProtocolHandler.mkDir(mkDirRequest);
			
			return mkDirResponse;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Request a touch operation to the Controller
	 * 1. Send the request to the Controller
	 * 2. Wait for the response from the Controller
	 * @param build
	 * @return
	 */
	public static TouchControllerResponseProto touch(TouchClientRequestProto touchRequest) {
		try {
			// Requests the touch operation
			FileOperationReqProtocol controllerProtocolHandler = new FileOperationReqProtocolImpl();
			TouchControllerResponseProto touchResponse = controllerProtocolHandler.touch(touchRequest);
			
			return touchResponse;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Writes a file into the DFS Filesystem
	 * 1. Requests the write file operation to the Controller;
	 * 2. Controller will send back an operation identifier;
	 * 3. Create a new semaphore with that operation id and the file content.
	 * 		The datanode will contact the client listener to start the file transfer
	 * 		Only return from this method when the transfer is marked as completed in the semaphore
	 * 
	 * @param writerProto
	 * @param content
	 */
	public static void write(FileWriteClientRequestProto writerProto, FileContent content) throws Exception {
		try {
			Logger.logDebug("Requesting write operation to the Controller", FileOperationsComm.class);
			// requests the operation to the controller
			FileOperationReqProtocol controllerProtocolHandler = new FileOperationReqProtocolImpl();
			FileWriteControllerResponseProto fileWriteResponseProto = controllerProtocolHandler.write(writerProto);
			
			// check if there was an error
			if (fileWriteResponseProto.getErrorMessage() != null && !fileWriteResponseProto.getErrorMessage().isEmpty()) {
				throw new Exception(fileWriteResponseProto.getErrorMessage());
			}
			// Create a new semaphore and waits.
			// The sending of the file will be handled by a listener, waiting for the datanode
			Logger.logDebug("Creating a new semaphore for write operation", FileOperationsComm.class);
			int operationId = fileWriteResponseProto.getOperationIdentifier().getId();
			DatanodeOperationSemaphore semaphore = SemaphoresHolder.addSempahore(operationId, OperationType.FILE_OP_WRITE, content);
			
			synchronized (semaphore) {
				Logger.logDebug("Waiting for semaphore to be release, i.e. for the datanode to respond", FileOperationsComm.class);
				semaphore.wait();
			}
			
			Logger.logDebug("Done sending the DFSFile", FileOperationsComm.class);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
	}
	
	/**
	 * Reads a file from the DFS Filesystem
	 * 1. Requests the read file operation to the Controller;
	 * 2. Controller will send back an operation identifier;
	 * 3. Create a new semaphore with that operation id and the file content.
	 * 		The datanode will contact the client listener to start the file transfer
	 * 		Only return from this method when the transfer is marked as completed in the semaphore
	 */
	public static byte[] read(FileReadClientRequestProto readProto) throws Exception {
		byte[] bytesRead = null;
		try {
			// requests the operation to the controller
			Logger.logDebug("Requesting read operation to the Controller", FileOperationsComm.class);
			FileOperationReqProtocol controllerProtocolHandler = new FileOperationReqProtocolImpl();
			FileReadControllerResponseProto fileReadResponseProto = controllerProtocolHandler.read(readProto);
			
			// check if there was an error
			if (fileReadResponseProto.getErrorMessage() != null && !fileReadResponseProto.getErrorMessage().isEmpty()) {
				throw new Exception(fileReadResponseProto.getErrorMessage());
			}
			// Create a new semaphore and waits.
			// The sending of the file will be handled by a listener, waiting for the datanode
			Logger.logDebug("Creating a new semaphore for read operation", FileOperationsComm.class);
			int operationId = fileReadResponseProto.getOperationIdentifier().getId();
			DatanodeOperationSemaphore semaphore = SemaphoresHolder.addSempahore(operationId, OperationType.FILE_OP_READ);
			
			synchronized (semaphore) {
				Logger.logDebug("Waiting for semaphore to be release, i.e. for the datanode to respond", FileOperationsComm.class);
				semaphore.wait();
			}
			if (semaphore.getResponse() != null) {
				bytesRead = (byte[])semaphore.getResponse();
				Logger.logStats("Network Data Request: read " + bytesRead.length + " bytes from the network", FileOperationsComm.class);
				Logger.logDebug("Datanode finished responding, got " + bytesRead.length  + " bytes", FileOperationsComm.class);
			} else {
				Logger.logDebug("ERROR: Datanode finished repsonding, but we have no file", FileOperationsComm.class);
			}
			
			// remove the semaphore from the memory
			SemaphoresHolder.removeSemaphore(operationId);
			
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		return bytesRead;
	}
	
	
	/**
	 * DEPRECATED
	 *  Requests an ls operation for the DFS
	 *  1. Requests the ls operation to the controller
	 *  2. Controller will send back an operation identifier
	 *  3. Create a new semaphore with that operation id and waits for a
	 *  	datanode to contact the listener and send a response
	 *  
	 * @param path
	 * 
	 */
	@Deprecated
	public static FileInfoLsProto ls(FilePathProto path) {
		try {
			
			// Requests the ls operation
			FileOperationReqProtocol controllerProtocolHandler = new FileOperationReqProtocolImpl();
			OperationIdentifierProto operationIdentifierProto = controllerProtocolHandler.ls(path);
			
			// Create a new semaphore and waits for a response
			//		(will be handled by the client listener)
			int operationId = operationIdentifierProto.getId();
			DatanodeOperationSemaphore sempahore = SemaphoresHolder.addSempahore(operationId, OperationType.FILE_OP_LS);
			synchronized (sempahore) {
				sempahore.wait();
			}
			
			return (FileInfoLsProto) sempahore.getResponse();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
}
