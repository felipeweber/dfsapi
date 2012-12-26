package dfs.client.api;

import java.util.ArrayList;
import java.util.List;

import dfs.client.comm.fileoperations.FileOperationsComm;
import dfs.client.fileoperations.FileOperationImpl;
import dfs.client.support.FileContent;
import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchControllerResponseProto;

/**
 * API to perform common operations over the DFS files
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DFSFileManager {

	/**
	 * Creates a directory in the DFS File Structure
	 * If the directory has inexistent parents, the whole tree will be created
	 * This change is limited to the Controller 
	 * 
	 * @param Path to create the directory in
	 * @return null if successful or an Error message
	 */
	public static String mkdir(String path) {
		MkDirClientRequestProto.Builder mkDirProto = MkDirClientRequestProto.newBuilder();
		mkDirProto.setPath(path);
		
		MkDirControllerResponseProto mkDirResponse = FileOperationsComm.mkDir(mkDirProto.build());

		return mkDirResponse.getErrorMessage();
	}
	
	/**
	 * Creates an empty file in the DFS File Structure
	 * If the file path has inexistent parents, the whole tree will be created
	 * This change is limited to the Controller
	 * 
	 * @param Complete path of the file
	 * @param Name of the file
	 * @return null if successful or an Error message
	 */
	public static String touch(String path, String filename) {
		TouchClientRequestProto.Builder touchProto = TouchClientRequestProto.newBuilder();
		touchProto.setPath(path);
		touchProto.setFilename(filename);
		
		TouchControllerResponseProto touchResponse = FileOperationsComm.touch(touchProto.build());

		return touchResponse.getErrorMessage();
	}
	
	/**
	 * Writes an entire file into the DFS Filesystem
	 * If the file already exists, it will be overwritten
	 * If the file doesn't exist, it will be created
	 * 
	 * @param path
	 * @param filename
	 * @param content
	 */
	public static void write(String path, String filename, byte[] content) throws Exception {
		Logger.logDebug("Started write operation over the DFS", DFSFileManager.class);
		FileWriteClientRequestProto.Builder writerProto = FileWriteClientRequestProto.newBuilder();
		writerProto.setPath(path);
		writerProto.setFilename(filename);
		writerProto.setTotalFileSize(content.length);
		
		FileContent fileContent = new FileContent(content);
		
		try {
			FileOperationsComm.write(writerProto.build(), fileContent);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	// TODO: write operation with offset and size
	public static void write(String path, String filename, int offset, int size, byte[] content) {
		return;
	}
	
	/**
	 * Read a whole file
	 */
	public static byte[] read(String path, String filename) throws Exception {
		return read(path, filename, 0, 0);
	}
	
	public static byte[] read(String path, String filename, int offset, int size) throws Exception {
		Logger.logDebug("Started read operation over the DFS", DFSFileManager.class);
		Logger.logStats("File Manager: starting read operation", DFSFileManager.class);
		byte[] bytesRead = null;
		try {
			bytesRead = FileOperationImpl.read(path, filename, offset, size);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		Logger.logStats("File Manager: ended read operation", DFSFileManager.class);
		return bytesRead;
	}
	

	/**
	 * Returns the list of files and folders from a given parent @path
	 * 	as a List of Strings
	 * 
	 * @param path
	 * @return List of Strings filepaths + names 
	 */
	@Deprecated
	public static List<String> ls(String path) {
		 FilePathProto.Builder filePathProto = FilePathProto.newBuilder();
		 filePathProto.setPath(path);
		 
		 FileInfoLsProto fileInfoLs = FileOperationsComm.ls(filePathProto.build());
		 
		 List<String> res = new ArrayList<String>();
		 for (FileInfoProto fileInfo : fileInfoLs.getFileList()) {
			res.add(fileInfo.getPath() + fileInfo.getFilename());
		}
		
		return res;
	}
	
}
