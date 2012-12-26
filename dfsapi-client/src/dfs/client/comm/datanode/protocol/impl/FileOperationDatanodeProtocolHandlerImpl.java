package dfs.client.comm.datanode.protocol.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

import dfs.client.comm.datanode.protocol.FileOperationDatanodeProtocolHandler;
import dfs.client.comm.state.DatanodeOperationSemaphore;
import dfs.client.comm.state.SemaphoresHolder;
import dfs.client.config.ClientConfigurations;
import dfs.client.support.FileContent;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.OperationType;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;

/**
 * Implements Protocol responsible between communicating client and datanodes
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationDatanodeProtocolHandlerImpl implements FileOperationDatanodeProtocolHandler {

	private DataInputStream in;
	private DataOutputStream out;
	
	/** MAXIMUM amount of data that will be sent at once */
	private static int NETWORK_MAX_BLOCK_SIZE = 64 * 1024; 
	
	public FileOperationDatanodeProtocolHandlerImpl(DataInputStream in, DataOutputStream out) {
		this.in = in;
		this.out = out;
	}
	
	public void startProtocol() throws Exception {
		
		// 1. wait for the datanode to send the operation id of the response
		int operationId = in.readInt();
		
		// get the semaphore
		// there are occasions where the datanode might start a connection without
		//	the client ended communicating with the Controller. in this case, the semaphore
		//	will not have been created yet. We have to retry until the semaphore is created
		DatanodeOperationSemaphore semaphore = null;
		while(semaphore == null) {
			semaphore = SemaphoresHolder.getSemaphore(operationId);
			if (semaphore == null) {
				Logger.logDebug("DATANODE RESPONSE: sempahore is null, waiting 50 milli secs before retrying", getClass());
				Thread.sleep(50);
			}
		}
		
		// act according to the operation type
		switch(semaphore.getOperationType()) {
		case OperationType.FILE_OP_LS:
			ls(semaphore);
			break;
		case OperationType.FILE_OP_WRITE:
			write(semaphore);
			break;
		case OperationType.FILE_OP_READ:
			read(semaphore);
			break;
		}
		
		synchronized (semaphore) {
			// release the semaphore
			semaphore.notify();
		}
		
	}
	
	private void write(DatanodeOperationSemaphore semaphore) throws Exception {
		// sends the file contents to the datanode
		FileContent fileContent = (FileContent) semaphore.getParameter();
		int bytesSent = 0;
		int offset = 0;
		int actuallySend;
		while(bytesSent < fileContent.getContent().length) {
			offset = bytesSent;
			bytesSent += NETWORK_MAX_BLOCK_SIZE;
			
			// the last chunk of data might be greater than the block size, 
			//	but we need to send just what's necessary
			if (bytesSent > fileContent.getContent().length)
				actuallySend = fileContent.getContent().length - bytesSent + NETWORK_MAX_BLOCK_SIZE;
			else
				actuallySend = NETWORK_MAX_BLOCK_SIZE;
			
			out.write(fileContent.getContent(), offset, actuallySend);
		}
	}
	
	private void read(DatanodeOperationSemaphore semaphore) throws Exception {
		// 2. Receive the total size of the data block
		int fileSize = in.readInt();
		
		// we'll discard the last byte, this is just to facilitate receiving the content directly into this byte
		byte[] fileContent = new byte[fileSize+1];
		
		// Reads the file contents that the datanode is sending.
		Logger.logDebug("Starting receiving data from datanode. Expect to get " + fileSize + " bytes of data", getClass());
		int actualRead, totalRead=0, bytesToRead;
		while(true) {
			// 3. Read the bytes from the datanode into the fileContent
			// The last package will probably have a size smaller than ProcotolConfigurations.MAX_READ_BLOCK_SIZE
			if (totalRead+ClientConfigurations.MAX_READ_BLOCK_SIZE > fileSize) {
				bytesToRead = fileSize-totalRead+1;
			} else {
				bytesToRead = ClientConfigurations.MAX_READ_BLOCK_SIZE;
			}
			actualRead = in.read(fileContent, totalRead, bytesToRead);
			if (actualRead == -1) {
				// ended reading the package
				break;
			}
			Logger.logDebug("Read " + actualRead + " bytes from datanode; fileContent writing started at offset " + totalRead, getClass());
			totalRead += actualRead;
		}
		Logger.logDebug("Ended reading bytes from datanode. Total read " + totalRead, getClass());
		
		// TODO check file integrity (compare totalRead with totalSize is a good start! A hash should be the next step)
		
		// set the fileContent as the semaphore response
		semaphore.setResponse(Arrays.copyOf(fileContent, fileSize));
	}
	
	private void ls(DatanodeOperationSemaphore semaphore) throws Exception {
		// 1. wait for the datanode to send the response
		FileInfoLsProto response = FileInfoLsProto.parseDelimitedFrom(in);
		
		// set the response
		semaphore.setResponse(response);
		
		// release the semaphore
		synchronized (semaphore) {
			semaphore.notify();
		}
	}
	
}
