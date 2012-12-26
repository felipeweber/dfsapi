package dfs.controller.filestructure.operations;

import java.util.List;

import dfs.common.exceptions.FileLockException;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.controller.filestructure.DFSFile;

public interface FileStructureOperation {

	public List<String> ls(String path);

	public String mkDir(String path);
	
	public String touch(String path, String filename);
	
	public DFSFile aquireWriteLock(String path, String filename, int fileSize) throws FileLockException, Exception;
	
	public DFSFile aquireReadLock(String path, String filename, String aquireeId) throws FileLockException, Exception;
	
	public List<String> findValidDatanodes(String path, String filename) throws Exception;

	public void parseWriteOperationResponse(String filepath, String filename, String datanodeId, String errorMessage) throws Exception;
	
	public void parseReadOperationResponse(String filepath, String filename, String datanodeId, String errorMessage, String lockAquireeId) throws Exception;

	public DFSFile retrieveFileInfo(String path, String filename);

}
