package dfs.datanode.filesystem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;

public interface FileSystemInterface {

	public DatanodeDFSFile findFileByFileKey(BigInteger fileKey);
	
	public DatanodeDFSFile createFile(BigInteger fileKey);
	
	public RandomAccessFile accessDataBlock(BigInteger fileGlobalKey, boolean writeMode) throws IOException;
	
	/********* OLD ***********/
	@Deprecated
	public FileInfoLsProto ls(String path);

	
}
