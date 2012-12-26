package dfs.datanode.comm.client.operation;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;

public interface FileOperations {

	public FileInfoLsProto performLs(FileOperationLsRequestProto requestProto) throws Exception;
	
}
