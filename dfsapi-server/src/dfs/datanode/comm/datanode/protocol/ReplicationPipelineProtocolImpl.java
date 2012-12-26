package dfs.datanode.comm.datanode.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import com.google.protobuf.ByteString;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.common.protocol.configurations.ReplicationPipelineOperationType;
import dfs.common.protocol.objects.FileOperationProtos.OperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineOperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.ReplicationPipelineStartFileWriteProto;
import dfs.datanode.DatanodeConfigurations;
import dfs.datanode.comm.datanode.DatanodeConnection;
import dfs.datanode.comm.replication.send.DataBlockSend;
import dfs.datanode.filesystem.manager.FileSystemManager;

/**
 * Protocol implementation of the sender side from the Replication Pipeline
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ReplicationPipelineProtocolImpl {

	private DatanodeConnection conn;
	private DataOutputStream out;
	private DataInputStream in;
	private DataBlockSend dataBlockSend;
	
	private void openConnection(DatanodeInfo datanodeInfo, String internalConnectionType) throws Exception {
		conn = new DatanodeConnection(datanodeInfo, internalConnectionType);
		if (!conn.connectOrReconnect()) {
			Logger.logError("Cannot connect to datanode " + datanodeInfo.getId() + " : " + datanodeInfo.getHost() + " : " + datanodeInfo.getStartPort(), this.getClass());
			throw new Exception("ERROR: Cannot connect to datanode " + datanodeInfo.getId() + " : " + datanodeInfo.getHost() + " : " + datanodeInfo.getStartPort());
		}
		
		out = new DataOutputStream(conn.getOutputStream());
		in = new DataInputStream(conn.getInputStream());
	}
	
	public ReplicationPipelineProtocolImpl(DataBlockSend dataBlockSend) throws Exception {
		this.dataBlockSend = dataBlockSend;
		// 1. Open connection with the nextDatanode on the Replication Pipeline
		openConnection(dataBlockSend.getNextDatanodeInfo(), InternalConnectionType.DATANODES_REPLICATION_PIPELINE_OPERATIONS);
	}

	public void writeFileNextDatanode() throws Exception {
		// build the operation request to send to the next datanode
		OperationIdentifierProto.Builder operationIdentifier = OperationIdentifierProto.newBuilder();
		operationIdentifier.setId(dataBlockSend.getOperationId());
		
		ReplicationPipelineOperationIdentifierProto.Builder repPipeOperationIdentifier = ReplicationPipelineOperationIdentifierProto.newBuilder();
		repPipeOperationIdentifier.setReplicationOperationTypeId(ReplicationPipelineOperationType.WRITE_OPERATION);
		repPipeOperationIdentifier.setOperationIdentifier(operationIdentifier.build());
		String nextDatanodeId = dataBlockSend.getNextDatanodeInfo().getId();
		Logger.logDebug("Sending request to write on the Replication Pipeline with datanode " + nextDatanodeId, getClass());
		// 1. Send a request to the nextDatanode to start sending a file through the Replication Pipeline
		repPipeOperationIdentifier.build().writeDelimitedTo(out);
		
		// 2. Send info on the file we're about to send
		ReplicationPipelineStartFileWriteProto.Builder repPipeStartWrite = ReplicationPipelineStartFileWriteProto.newBuilder();
		repPipeStartWrite.setFileKey(ByteString.copyFrom(dataBlockSend.getGlobalFileKey().toByteArray()));
		repPipeStartWrite.setFileSize(dataBlockSend.getTotalFileSize());
		repPipeStartWrite.build().writeDelimitedTo(out);
		
		// Load up a virtual reference of the file we need to send
		FileSystemManager fileSystemManager = FileSystemManager.getInstance();
		fileSystemManager.startDataBlockRead(dataBlockSend.getGlobalFileKey());
		
		// Read from the local block and send it to the next datanode
		int totalBytesSent = 0;
		int bytesRead = 0;
		byte[] bytesToSend = new byte[DatanodeConfigurations.NETWORK_MAX_BLOCK_SIZE];
		int offset = 0;
		int totalFileSize = dataBlockSend.getTotalFileSize();
		Logger.logDebug("Start sending " + totalFileSize + " bytes of data to datanode " + nextDatanodeId, getClass());
		while(totalBytesSent < totalFileSize) {
			offset = totalBytesSent;
			bytesRead = fileSystemManager.readBlock(bytesToSend, dataBlockSend.getGlobalFileKey(), offset, DatanodeConfigurations.NETWORK_MAX_BLOCK_SIZE);
			totalBytesSent += bytesRead;
			// *3. Indicate there's more data to send
			/*Logger.logDebug("Sending more data signal to datanode " + nextDatanodeId, getClass());
			ReplicationPipelineDataBlockWriteProto.Builder repPipeDataWrite = ReplicationPipelineDataBlockWriteProto.newBuilder();
			repPipeDataWrite.setMoreData(1);
			repPipeDataWrite.build().writeDelimitedTo(out);*/
			// *4. Send the data to the next datanode
			Logger.logDebug("Sending " + bytesRead + " to datanode " + nextDatanodeId, getClass());
			out.write(bytesToSend, 0, bytesRead);
		}
		
		fileSystemManager.endDataBlockRead(dataBlockSend.getGlobalFileKey());
		
		// 5. Indicate there's NO more data to send
		/*Logger.logDebug("Sending NO more data signal to datanode " + nextDatanodeId, getClass());
		ReplicationPipelineDataBlockWriteProto.Builder repPipeDataWrite = ReplicationPipelineDataBlockWriteProto.newBuilder();
		repPipeDataWrite.setMoreData(0);
		repPipeDataWrite.build().writeDelimitedTo(out);*/
		
		Logger.logDebug("Ended write operation on the Replication Pipeline with datanode " + nextDatanodeId, getClass());
		
		try {
			out.close();
			in.close();
		} catch (Exception e) {
			Logger.logError("ERROR CLOSING CONNECTION STREAMS", e, getClass());
		}
		
		
	}
	
	
}
