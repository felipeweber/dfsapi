package dfs.controller.filestructure.operations.impl;

import java.util.List;

import dfs.common.exceptions.FileLockException;
import dfs.common.logger.Logger;
import dfs.controller.filestructure.DFSFile;
import dfs.controller.filestructure.FileStructure;
import dfs.controller.filestructure.operations.FileStructureOperation;

public class FileStructureOperationImpl implements FileStructureOperation {

	public List<String> ls(String path) {
		//return FileStructure.findDFSFileByPathString(path).getValidDatanodesId();
		return null;
	}
	
	public String mkDir(String path) {
		try {
			return FileStructure.getInstance().mkDir(path);
		} catch (Exception e) {
			e.printStackTrace();
			return "INTERNAL SERVER ERROR " + e.getMessage();
		}
	}
	
	public String touch(String path, String filename) {
		try {
			return FileStructure.getInstance().touch(path, filename);
		} catch (Exception e) {
			e.printStackTrace();
			return "ERROR " + e.getMessage();
		}
	}
	
	public DFSFile aquireWriteLock(String path, String filename, int fileSize) throws FileLockException, Exception {
		try {
			return FileStructure.getInstance().aquireWriteLock(path, filename, fileSize);
		} catch (FileLockException fle) {
			throw fle;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public DFSFile aquireReadLock(String path, String filename, String aquireeId) throws FileLockException, Exception {
		try {
			return FileStructure.getInstance().aquireReadLock(path, filename, aquireeId);
		} catch (FileLockException fle) {
			throw fle;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}
	
	public List<String> findValidDatanodes(String path, String filename) throws Exception {
		try {
			return FileStructure.getInstance().findValidDatanodes(path, filename);
		} catch (Exception e) {
			System.out.println("INTERNAL SERVER ERROR " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}
	
	/**
	 * Perform file structure modifications according to a response from a Write 
	 * operation performed by a client over a datanode
	 */
	public void parseWriteOperationResponse(String filepath, String filename,
			String datanodeId, String errorMessage) throws Exception {
		// TODO what if there's an error??
		boolean hasNoErrors = errorMessage == null || errorMessage.trim().isEmpty();
		FileStructure.getInstance().revalidateWrittenToFile(filepath, filename, datanodeId, hasNoErrors);
		
	}

	/**
	 * Perform file structure modifications according to a response from a Read 
	 * operation performed by a client over a datanode
	 */
	public void parseReadOperationResponse(String filepath, String filename, String datanodeId, String errorMessage, String lockAquireeId) throws Exception {
		boolean hasNoErrors = errorMessage!=null && !errorMessage.isEmpty();
		FileStructure.getInstance().revalidateReadFromFile(filepath, filename, datanodeId, hasNoErrors, lockAquireeId);
		
	}
	
	public DFSFile retrieveFileInfo(String path, String filename) {
		try {
			return FileStructure.getInstance().retrieveFileInfo(path, filename);
		} catch (Exception e) {
			Logger.logError("ERROR GETTING FILE STRUCTURE ISNTANCE", e, getClass());
			return null;
		}
	}
	
}
