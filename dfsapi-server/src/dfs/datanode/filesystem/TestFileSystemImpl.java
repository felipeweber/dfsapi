package dfs.datanode.filesystem;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;


public class TestFileSystemImpl  {

	public FileInfoLsProto ls(String path) {
		FileInfoLsProto.Builder fileInfoLs = FileInfoLsProto.newBuilder();
		
		FilePathProto.Builder filePath = FilePathProto.newBuilder();
		fileInfoLs.setStartingpath(filePath.setPath("/").build());
		
		fileInfoLs.addFile(FileInfoProto.newBuilder().setFilename("bin/").setPath(filePath.setPath("/").build()));
		fileInfoLs.addFile(FileInfoProto.newBuilder().setFilename("etc/").setPath(filePath.setPath("/").build()));
		fileInfoLs.addFile(FileInfoProto.newBuilder().setFilename("home/").setPath(filePath.setPath("/").build()));
		fileInfoLs.addFile(FileInfoProto.newBuilder().setFilename("usr/").setPath(filePath.setPath("/").build()));
		
		return fileInfoLs.build();
	}
	
}
