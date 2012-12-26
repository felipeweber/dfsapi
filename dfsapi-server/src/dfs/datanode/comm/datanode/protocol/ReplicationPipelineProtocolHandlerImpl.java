package dfs.datanode.comm.datanode.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;

import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.ReplicationPipelineOperationType;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineOperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineStartFileWriteProto;
import dfs.datanode.comm.controller.operation.FileOperationsRes;
import dfs.datanode.comm.controller.operation.impl.FileOperationsResImpl;
import dfs.datanode.filesystem.manager.FileSystemManager;

/**
 * Implements the receiver side of a Replication Pipeline between Datanodes
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ReplicationPipelineProtocolHandlerImpl {

	// maximum of bytes that will be read at once when receiving a file
	private static int MAX_READ_BLOCK_SIZE = 10*1024;
	
	private int operationId;
	private DataInputStream in;
	private DataOutputStream out;
	
	public ReplicationPipelineProtocolHandlerImpl(DataInputStream in, DataOutputStream out) {
		this.in = in;
		this.out = out;
	}
	
	public void startProtocol() throws Exception {
		
		// 1. waits for the other datanode to send an operation code
		ReplicationPipelineOperationIdentifierProto replicationPipelineOperationIdentifierProto = ReplicationPipelineOperationIdentifierProto.parseDelimitedFrom(in);
		operationId = replicationPipelineOperationIdentifierProto.getOperationIdentifier().getId();
		switch (replicationPipelineOperationIdentifierProto.getReplicationOperationTypeId()) {
		case ReplicationPipelineOperationType.WRITE_OPERATION:
			write();
			break;
		}
	}
	
	public void write() throws Exception {
		String errorMessage = null;
		try {
			Logger.logDebug("Starting a new write operation on the Replication Pipeline", this.getClass());
			// 1. Receive the operation details (fileGlobalKey and fileSize)
			ReplicationPipelineStartFileWriteProto startFileWriteProto = ReplicationPipelineStartFileWriteProto.parseDelimitedFrom(in); 
			
			BigInteger fileGlobalKey = new BigInteger(startFileWriteProto.getFileKey().toByteArray());
			int totalFileSize = startFileWriteProto.getFileSize();
			
			FileSystemManager fileSystemManager = FileSystemManager.getInstance();
			// perform the tasks necessary to initiate a file write operation
			fileSystemManager.startDataBlockWrite(fileGlobalKey, totalFileSize);
			
			// Reads the file contents that the datanode is sending.
			Logger.logDebug("Starting receiving data from datanode on Replication Pipeline. Total file size: " + totalFileSize, getClass());
			byte[] byteRead = new byte[MAX_READ_BLOCK_SIZE];
			int actualRead, totalRead=0;
			while(true) {
				// *2. Receive from the datanode stats if we have more data packages to read or not
				/*ReplicationPipelineDataBlockWriteProto repPipeDataWrite = ReplicationPipelineDataBlockWriteProto.parseDelimitedFrom(in);
				if (repPipeDataWrite.getMoreData() != 1) {
					Logger.logDebug("Received 'no more data' signal from datanode", getClass());
					break;
				}*/
				actualRead = in.read(byteRead, 0, byteRead.length);
				if (actualRead == -1) {
					// ended reading the package
					break;
				}
				Logger.logDebug("Read " + actualRead + " bytes from datanode; writing starting at offset " + totalRead, getClass());
				// write block to disk
				// totalRead will be used as the file offset, as we expect to receive the file ordered
				fileSystemManager.writeBlock(fileGlobalKey, byteRead, totalRead, actualRead);
				totalRead += actualRead;
			}
			Logger.logDebug("Ended reading bytes from datanode. Total read " + totalRead, getClass());
			
			// TODO check file integrity (compare totalRead with totalSize is a good start! A hash should be the next step)
			
			fileSystemManager.endDataBlockWrite(Long.valueOf(operationId), fileGlobalKey, totalFileSize);
		} catch (Exception e) {
			Logger.logError("ERROR DURING WRITE OPERATION ON REPLICATION PIPELINE", e, getClass());
			errorMessage = e.getMessage();
		}
		// let the Controller know the file is updated (or not) on this datanode
		FileOperationsRes fileOperationsRes = new FileOperationsResImpl();
		fileOperationsRes.sendWriteReponse(operationId, errorMessage);
		
		Logger.logDebug("Finished write operation on the Replication Pipeline", this.getClass());
		
	}
	
	
}
