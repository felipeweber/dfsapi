package dfs.datanode.comm.client.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;

import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerRequestProto;
import dfs.datanode.DatanodeConfigurations;
import dfs.datanode.comm.client.ClientConnection;
import dfs.datanode.comm.client.protocol.FileOperationProtocol;
import dfs.datanode.filesystem.manager.FileSystemManager;

public class FileOperationProtocolImpl implements FileOperationProtocol {

	
	private ClientConnection conn = null;
	private DataOutputStream out;
	private DataInputStream in;

	// maximum of bytes that will be read at once when receiving a file
	private static int MAX_READ_BLOCK_SIZE = 10*1024;

	private void openConnection(int clientId, String clientHost, int clientPort) throws Exception {
		if (conn == null) {
			conn = new ClientConnection(clientId, clientHost, clientPort);
		}
		if (!conn.connectOrReconnect()) {
			Logger.logError("Cannot connect to client " + clientId + " : " + clientHost + " : " + clientPort, this.getClass());
			throw new Exception("ERROR: Cannot connect to client " + clientId + " : " + clientHost + " : " + clientPort);
		}
		
		out = new DataOutputStream(conn.getOutputStream());
		in = new DataInputStream(conn.getInputStream());
	}

	public void writeToFile(FileWriteControllerRequestProto fileWriteRequest) throws Exception {
		Logger.logDebug("Starting write request protocol with client", this.getClass());
		
		// Global identifier for the file to write to (sent by the Controller)
		BigInteger fileGlobalKey = new BigInteger(fileWriteRequest.getFilekey().toByteArray());
		
		FileSystemManager fileSystemManager = FileSystemManager.getInstance();
		// perform the tasks necessary to initiate a file write operation
		fileSystemManager.startDataBlockWrite(fileGlobalKey, fileWriteRequest.getTotalFileSize());
		
		// connect to the client
		openConnection(fileWriteRequest.getClientId(), fileWriteRequest.getClientHost(), fileWriteRequest.getClientPort());
		
		// 1. send operation id
		out.writeInt(fileWriteRequest.getOperationId());
		
		// Reads the file contents that the client is sending.
		// we trust that the sender will only send what we need to read
		Logger.logDebug("Starting receiving data from client. Total file size: " + fileWriteRequest.getTotalFileSize(), getClass());
		byte[] byteRead = new byte[MAX_READ_BLOCK_SIZE];
		int actualRead, totalRead=0;
		while(true) {
			actualRead = in.read(byteRead, 0, byteRead.length);
			if (actualRead == -1) break;
			Logger.logDebug("Read " + actualRead + " bytes from client; writing starting at offset " + totalRead, getClass());
			// totalRead will be used as the file offset, as we expect to receive the file ordered
			fileSystemManager.writeBlock(fileGlobalKey, byteRead, totalRead, actualRead);
			totalRead += actualRead;
		}
		Logger.logDebug("Ended reading bytes from client. Total read " + totalRead, getClass());
		
		// TODO check file integrity (compare totalRead with totalSize is a good start! A hash should be the next step)
		
		fileSystemManager.endDataBlockWrite(Long.valueOf(fileWriteRequest.getOperationId()), fileGlobalKey, fileWriteRequest.getTotalFileSize());
		
		out.close();
		in.close();
		conn.close();
		
		Logger.logDebug("Ended write request protocol with client", this.getClass());
	}
	
	
	public void readFromFile(FileReadControllerRequestProto fileReadRequest)  throws Exception {
		Logger.logDebug("Starting read request protocol with client", this.getClass());
		
		// Global identifier for the file to read from (sent by the Controller)
		BigInteger fileGlobalKey = new BigInteger(fileReadRequest.getFilekey().toByteArray());
		int readOffset = fileReadRequest.getOffset();
		int readSize = fileReadRequest.getSize();
		
		// Load up a virtual reference of the file (or part of the file) that we need to send,
		//	while also getting it's total size
		FileSystemManager fileSystemManager = FileSystemManager.getInstance();
		int totalFileSize = fileSystemManager.startDataBlockRead(fileGlobalKey, readOffset, readSize);
		
		// connect to the client
		openConnection(fileReadRequest.getClientId(), fileReadRequest.getClientHost(), fileReadRequest.getClientPort());
		
		// 1. send operation id
		out.writeInt(fileReadRequest.getOperationId());
		
		// 2. Send the file size to the client
		out.writeInt(totalFileSize);
		
		// Read from the local block and send it to the client
		int totalBytesSent = 0;
		int noBytesRead = 0;
		byte[] bytesToSend = new byte[DatanodeConfigurations.NETWORK_MAX_BLOCK_SIZE];
		int localBlockOffset = readOffset;
		int totalBytesToSend = readOffset == 0 && readSize == 0 ? totalFileSize : readSize;
		int noBytesToRead;
		Logger.logDebug("Start sending " + totalFileSize + " bytes of data to client " + fileReadRequest.getClientId(), getClass());
		while(totalBytesSent < totalBytesToSend) {
			localBlockOffset = totalBytesSent+readOffset;
			
			if ((readOffset == 0 && readSize == 0)
					|| ((localBlockOffset + DatanodeConfigurations.NETWORK_MAX_BLOCK_SIZE) < (readOffset + readSize))) {
				noBytesToRead = DatanodeConfigurations.NETWORK_MAX_BLOCK_SIZE;
			} else {
				// if we have a read limit offset and size, when we reach offset+size we should not read further data
				noBytesToRead = readOffset + readSize - localBlockOffset; 
			}
			
			noBytesRead = fileSystemManager.readBlock(bytesToSend, fileGlobalKey, localBlockOffset, noBytesToRead);
			totalBytesSent += noBytesRead;
			// 3. Send the data to the client
			if (noBytesRead > 0) {
				Logger.logDebug("Sending " + noBytesRead + " to client " + fileReadRequest.getClientId(), getClass());
				out.write(bytesToSend, 0, noBytesRead);
			} else {
				break;
			}
		}
		
		fileSystemManager.endDataBlockRead(fileGlobalKey);
		
		out.close();
		in.close();
		conn.close();
		
		Logger.logDebug("Ended read request protocol with client", this.getClass());
	}
	
	/**** OLD STUFF ****/
	
	@Deprecated
	public void respondLsRequest(FileOperationLsRequestProto lsRequestProto, FileInfoLsProto lsResponse) throws Exception {
		// connect to the client
		openConnection(lsRequestProto.getClientId(), lsRequestProto.getClientHost(), lsRequestProto.getClientPort());
		
		// 1. send operation id
		out.writeInt(lsRequestProto.getOperationId());
		
		// 2. send response
		lsResponse.writeDelimitedTo(out);
		
	}
}
