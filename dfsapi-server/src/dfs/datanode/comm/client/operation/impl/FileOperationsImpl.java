package dfs.datanode.comm.client.operation.impl;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileOperationLsRequestProto;
import dfs.datanode.comm.client.operation.FileOperations;

public class FileOperationsImpl implements FileOperations {

	public FileInfoLsProto performLs(FileOperationLsRequestProto requestProto) throws Exception {
		/*// Actually perform the ls
		FileSystemInterface fileSystem = new TestFileSystemImpl();
		FileInfoLsProto fileInfoLs = fileSystem.ls(requestProto.getPath().getPath());

		return fileInfoLs;*/
		return null;
	}
	
}
