package dfs.client.comm.controller.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import dfs.client.comm.controller.ControllerConnection;
import dfs.client.comm.controller.protocol.FileOperationReqProtocol;
import dfs.client.config.ClientConfigurations;
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

/**
 * Implementation of methods that manage the protocols between client and controller
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationReqProtocolImpl implements FileOperationReqProtocol {

	private void identifySelf(DataOutputStream out) throws IOException {
		ClientIdentifierProto.Builder clientIdentifier = ClientIdentifierProto.newBuilder();
		clientIdentifier.setId(ClientConfigurations.CLIENT_ID);
		clientIdentifier.setHost(ClientConfigurations.CLIENT_IP);
		clientIdentifier.setListenerPort(ClientConfigurations.FILE_OP_RES_LISTEN_PORT);
		
		clientIdentifier.build().writeDelimitedTo(out);
	}
	
	/**
	 * Sends an mkdir request to the controller server;
	 * Response will be an Error Message, null meaning success
	 * 
	 * 1. Send the operation code
	 * 2. Send the path
	 * 3. Waits for the response
	 */
	public MkDirControllerResponseProto mkDir(MkDirClientRequestProto mkDirRequestProto) throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}
		
		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_MKDIR);
		
		// 2. send the mkdir request
		mkDirRequestProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		MkDirControllerResponseProto mkDirResponseProto = MkDirControllerResponseProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return mkDirResponseProto;
	}
	
	/**
	 * Sends an touch request to the controller server;
	 * Response will be an Error Message, null meaning success
	 * 
	 * 1. Send the operation code
	 * 2. Send the path and filename
	 * 3. Waits for the response
	 */
	public TouchControllerResponseProto touch(TouchClientRequestProto touchRequestProto) throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}
		
		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_TOUCH);
		
		// 2. send the mkdir request
		touchRequestProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		TouchControllerResponseProto touchResponseProto = TouchControllerResponseProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return touchResponseProto;
	}
	
	/**
	 * Send a write request to the Controller.
	 * It will respond with an operation identifier, which we'll use in this
	 * clients listener when sending the file contents to a datanode that will
	 * start the communication for the transfer
	 */
	public FileWriteControllerResponseProto write(FileWriteClientRequestProto writerProto) throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}
		
		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_WRITE);
		
		// 2. send the path and filename
		writerProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		FileWriteControllerResponseProto fileWriteResponseProto = FileWriteControllerResponseProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return fileWriteResponseProto;
	}
	
	/**
	 * Send a read request to the Controller.
	 * It will respond with an operation identifier, which we'll use in this
	 * clients listener when reading the file contents from a datanode that will
	 * start the communication for the transfer
	 */
	public FileReadControllerResponseProto read(FileReadClientRequestProto readProto)  throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}

		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_READ);
		
		// 2. send the path and filename
		readProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		FileReadControllerResponseProto fileReadResponseProto = FileReadControllerResponseProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return fileReadResponseProto;
	}
	
	public int retrieveFileInfo(FileInfoReqProto fileInfoReqProto) throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}

		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_INFO);
		
		// 2. send the path and filename
		fileInfoReqProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		FileInfoResProto fileInfoResProto = FileInfoResProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return fileInfoResProto.getSize();
	}
	
	
	/**
	 * Sends an ls request to the controller server;
	 * it will respond with an operation identifier that we'll use
	 * internally while waiting for a datanode to respond
	 * 
	 * 1. Send the operation code
	 * 2. Send the path
	 * 3. Waits for the response (an operation id)
	 */
	@Deprecated
	public OperationIdentifierProto ls(FilePathProto filePathProto) throws Exception {
		ControllerConnection conn = new ControllerConnection();
		
		if (!conn.connectOrReconnectToControllerServer()) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			throw new Exception("ERROR: Cannot connect to controller server");
		}
		
		DataOutputStream out = new DataOutputStream(conn.getOutputStream());
		DataInputStream in = new DataInputStream(conn.getInputStream());
		
		// 0. all protocols must start by identifying to the controller
		identifySelf(out);
		
		// 1. send the operation code
		out.writeInt(OperationType.FILE_OP_LS);
		
		// 2. send the path
		filePathProto.writeDelimitedTo(out);
		
		// 3. waits for the response
		OperationIdentifierProto operationIdentifierProto = OperationIdentifierProto.parseDelimitedFrom(in);
		
		out.close();
		in.close();
		conn.close();
		
		return operationIdentifierProto;
	}
}
