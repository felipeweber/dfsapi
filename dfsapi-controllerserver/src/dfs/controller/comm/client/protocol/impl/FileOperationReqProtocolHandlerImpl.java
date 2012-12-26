package dfs.controller.comm.client.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import dfs.common.exceptions.FileLockException;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.ClientIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoReqProto;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoResProto;
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
import dfs.controller.comm.client.protocol.FileOperationReqProtocolHandler;
import dfs.controller.comm.state.CurrentOperations;
import dfs.controller.comm.state.parameters.FileReadOperationParameter;
import dfs.controller.comm.state.parameters.FileWriteOperationParameter;
import dfs.controller.fileoperations.FileOperations;
import dfs.controller.fileoperations.impl.FileOperationsImpl;
import dfs.controller.filestructure.DFSFile;
import dfs.controller.filestructure.operations.FileStructureOperation;
import dfs.controller.filestructure.operations.impl.FileStructureOperationImpl;

/**
 * Implements the controller server side of the protocol of communication
 * between controller and clients
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationReqProtocolHandlerImpl implements FileOperationReqProtocolHandler {

	private DataInputStream in;
	private DataOutputStream out;
	private ClientIdentifierProto clientIdentifierProto;
	
	public FileOperationReqProtocolHandlerImpl(DataInputStream in, DataOutputStream out) {
		this.in = in;
		this.out = out;
	}
	
	public void startProtocol() throws Exception {
		
		// 1. waits for the client to identify itself
		clientIdentifierProto = ClientIdentifierProto.parseDelimitedFrom(in);
		
		// 2. waits for the client to send an operation code
		int operationType = in.readInt();
		switch (operationType) {
		case OperationType.FILE_OP_LS:
			lsProtocol();
			break;
		case OperationType.FILE_OP_MKDIR:
			mkDirProtocol();
			break;
		case OperationType.FILE_OP_TOUCH:
			touchProtocol();
			break;
		case OperationType.FILE_OP_WRITE:
			writeProtocol();
			break;
		case OperationType.FILE_OP_READ:
			readProtocol();
			break;
		case OperationType.FILE_OP_INFO:
			fileInfoProtocol();
			break;
		}

	}
	
	private void mkDirProtocol() throws Exception {
		// 1. Receive path to create a directory in
		MkDirClientRequestProto mkDirRequestProto = MkDirClientRequestProto.parseDelimitedFrom(in);
		
		// 2. Create a new operation
		Long operationId = CurrentOperations.addOperation(clientIdentifierProto, OperationType.FILE_OP_MKDIR, mkDirRequestProto);
		
		// 3. Create the directory in the DFS File Structure
		FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
		String errorMessage = fileStructureOperation.mkDir(mkDirRequestProto.getPath());
		
		// build response protocol
		MkDirControllerResponseProto.Builder mkDirResponse = MkDirControllerResponseProto.newBuilder();
		mkDirResponse.setErrorMessage(errorMessage);
		
		// 4. Send response to the client
		mkDirResponse.build().writeDelimitedTo(out);
		
	}
	
	private void touchProtocol() throws Exception {
		// 1. Receive path and filename to be created
		TouchClientRequestProto touchRequestProto = TouchClientRequestProto.parseDelimitedFrom(in);
		
		// 2. Create a new operation
		Long operationId = CurrentOperations.addOperation(clientIdentifierProto, OperationType.FILE_OP_TOUCH, touchRequestProto);
		
		// 3. Create the file in the DFS File Structure
		FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
		String errorMessage = fileStructureOperation.touch(touchRequestProto.getPath(), touchRequestProto.getFilename());
		
		// build response protocol
		TouchControllerResponseProto.Builder touchResponse = TouchControllerResponseProto.newBuilder();
		touchResponse.setErrorMessage(errorMessage);
		
		// 4. Send response to the client
		touchResponse.build().writeDelimitedTo(out);
		
	}

	private void writeProtocol() throws Exception {
		// 1. Receive path and filename that will be written to
		FileWriteClientRequestProto fileWriteProto = FileWriteClientRequestProto.parseDelimitedFrom(in);
		
		FileWriteControllerResponseProto.Builder response = FileWriteControllerResponseProto.newBuilder(); 
		
		// 2. Check if the file can be written to
		DFSFile dfsFile = null;
		try {
			FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
			dfsFile = fileStructureOperation.aquireWriteLock(fileWriteProto.getPath(), fileWriteProto.getFilename(), fileWriteProto.getTotalFileSize());
		} catch (FileLockException fle) {
			// file is locked by someone else, respond that to the client
			response.setErrorMessage(fle.getMessage());
		} catch (Exception e) {
			response.setErrorMessage(e.getMessage());
			System.out.println("Error while trying to lock a file for a WRITE request");
			e.printStackTrace();
		}
		
		// 3. Create a new operation (the parameter of the operation is FileWriteOperationParameter)
		FileWriteOperationParameter fileWriteParm = new FileWriteOperationParameter(dfsFile, fileWriteProto.getPath(), fileWriteProto.getTotalFileSize());
		Long operationId = CurrentOperations.addOperation(clientIdentifierProto, OperationType.FILE_OP_WRITE, fileWriteParm);
		
		// 4. Respond the operation id and/or an error message to the client
		OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
		operationIdentifierProto.setId(operationId.intValue());
		response.setOperationIdentifier(operationIdentifierProto.build());
		response.build().writeDelimitedTo(out);
		
		if (response.getErrorMessage() == null || response.getErrorMessage().isEmpty()) {
			// 4. Send the write request to a datanode, which will communicate
			//  directly with the client
			FileOperations fileOperations = new FileOperationsImpl();
			fileOperations.requestFileOperation(operationId);
		}
		
	}
	
	private void readProtocol() throws Exception {
		// 1. Receive path and filename that will be written to
		FileReadClientRequestProto fileReadProto = FileReadClientRequestProto.parseDelimitedFrom(in);
		
		FileReadControllerResponseProto.Builder response = FileReadControllerResponseProto.newBuilder(); 
		
		// 2. Check if the file can be read from
		DFSFile dfsFile = null;
		try {
			FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
			dfsFile = fileStructureOperation.aquireReadLock(fileReadProto.getPath(), fileReadProto.getFilename(), String.valueOf(clientIdentifierProto.getId()));
		} catch (FileLockException fle) {
			// file is locked by someone else, respond that to the client
			response.setErrorMessage(fle.getMessage());
		} catch (Exception e) {
			response.setErrorMessage(e.getMessage());
			System.out.println("Error while trying to lock a file for a READ request");
			e.printStackTrace();
		}
		
		// 3. Create a new operation (the parameter of the operation is FileWriteOperationParameter)
		FileReadOperationParameter fileReadParm = new FileReadOperationParameter(dfsFile, fileReadProto.getPath(), fileReadProto.getOffset(), fileReadProto.getSize());
		Long operationId = CurrentOperations.addOperation(clientIdentifierProto, OperationType.FILE_OP_READ, fileReadParm);
		
		// 4. Respond the operation id and/or an error message to the client
		OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
		operationIdentifierProto.setId(operationId.intValue());
		response.setOperationIdentifier(operationIdentifierProto.build());
		response.build().writeDelimitedTo(out);
		
		if (response.getErrorMessage() == null || response.getErrorMessage().isEmpty()) {
			// 4. Send the read request to a datanode, which will communicate
			//  directly with the client
			FileOperations fileOperations = new FileOperationsImpl();
			fileOperations.requestFileOperation(operationId);
		}
		
	}
	
	private void fileInfoProtocol() throws Exception {
		// 1. Receive path and filename that will be written to
		FileInfoReqProto fileInfoReqProto = FileInfoReqProto.parseDelimitedFrom(in);
		
		FileInfoResProto.Builder fileInfoResProto = FileInfoResProto.newBuilder(); 
		
		// 2. Check if the file exists and get information on it
		DFSFile dfsFile = null;
		FileStructureOperation fileStructureOperation = new FileStructureOperationImpl();
		dfsFile = fileStructureOperation.retrieveFileInfo(fileInfoReqProto.getPath().getPath(), fileInfoReqProto.getFilename());
		if (dfsFile != null) {
			fileInfoResProto.setFilename(dfsFile.getName());
			fileInfoResProto.setPath(fileInfoReqProto.getPath());
			fileInfoResProto.setSize(dfsFile.getFileSize());
		}
		
		// 3. Respond to the client
		fileInfoResProto.build().writeDelimitedTo(out);
		
	}
	
	@Deprecated
	private void lsProtocol() throws Exception {
		// 1. Receive path to ls from
		FilePathProto filePath = FilePathProto.parseDelimitedFrom(in);
		
		// 2. Create a new operation
		Long operationId = CurrentOperations.addOperation(clientIdentifierProto, OperationType.FILE_OP_LS, filePath);
		
		// 3. Respond the operation id to the client
		OperationIdentifierProto.Builder operationIdentifierProto = OperationIdentifierProto.newBuilder();
		operationIdentifierProto.setId(operationId.intValue());
		operationIdentifierProto.build().writeDelimitedTo(out);
		
		// 4. Internally figure how to do the ls
		// 	and send the request to a datanode
		FileOperations fileOperations = new FileOperationsImpl();
		fileOperations.requestFileOperation(operationId);
		
	}
}
