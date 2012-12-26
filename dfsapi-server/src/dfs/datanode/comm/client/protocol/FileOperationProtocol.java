package dfs.datanode.comm.client.protocol;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerRequestProto;


public interface FileOperationProtocol {

	public void writeToFile(FileWriteControllerRequestProto fileWriteRequest) throws Exception;
	
	public void readFromFile(FileReadControllerRequestProto fileReadRequest)  throws Exception;
	
	@Deprecated
	public void respondLsRequest(FileOperationLsRequestProto lsRequestProto, FileInfoLsProto lsResponse) throws Exception;

	
}
