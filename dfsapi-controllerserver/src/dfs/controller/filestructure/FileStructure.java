package dfs.controller.filestructure;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dfs.common.exceptions.FileLockException;
import dfs.common.logger.Logger;

/**
 * Holds an in-memory copy of the current file structure of the DFS
 * This class cannot be instantiated. Use getInstance() instead
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileStructure {

	private static final FileStructure INSTANCE = new FileStructure();;
	
	/* Operations over the filestructure config file must be read/write synchronized:
	   May have multiple read operations at the same time, but writes are exclusive */
	private ReentrantReadWriteLock fileLock = new ReentrantReadWriteLock();
	private ReadLock fileReadLock = fileLock.readLock();
	private WriteLock fileWriteLock = fileLock.writeLock();
	
	/* Operations over the in memory tree must be read/write synchronized:
	   May have multiple read operations at the same time, but writes are exclusive */
	private ReentrantReadWriteLock treeLock = new ReentrantReadWriteLock();
	private ReadLock treeReadLock = treeLock.readLock();
	private WriteLock treeWriteLock = treeLock.writeLock();
	
	/* First node of the file structure */
	private DFSFile dfsFileRoot;
	
	
	public static FileStructure getInstance() throws Exception {
		return INSTANCE;
	}
	
	/** This class cannot be instantiated. Use getInstance() instead.
	 *  This constructor will be called only once while the Controller is running.
	 *  Opens the filestructure file.
	 *  Build the current File Structure of the DFS based on the filestructure file.
	 * @throws Exception 
	 **/
	private FileStructure() {
		try {
			loadFileStructureFromDisk();
		} catch (Exception e) {
			Logger.logError("CRITICAL ERROR: COULD NOT LOAD FILE STRUCTURE FROM DISK", e, getClass());
		}
	}
	
	/**
	 * Load the DFS File Structure from the Disk (a JSON file)
	 * @throws Exception 
	 */
	private void loadFileStructureFromDisk() throws Exception  {
		try {
			fileReadLock.lock();
			ObjectMapper mapper = new ObjectMapper();
			File f = new File("filestructure.json");
			// if a File Structure doesn't exist, create a new structure starting on "/"
			if (!f.exists()) {
				f.createNewFile();
				dfsFileRoot = new DFSFile();
				dfsFileRoot.setName("/");
				dfsFileRoot.setType('D');
				return;
			}
			dfsFileRoot = mapper.readValue(f, DFSFile.class);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			fileReadLock.unlock();
		}
	}
	/**
	 * Persist the current File Structure from RAM to Disk
	 */
	private void persistFileStructureToDisk() throws JsonGenerationException, JsonMappingException, IOException {
		try {
			fileWriteLock.lock();
			ObjectMapper mapper = new ObjectMapper();
			mapper.writeValue(new File("filestructure.json"), dfsFileRoot);
		} finally {
			fileWriteLock.unlock();
		}
	}
	
	/**
	 * Create a new directory in the file structure
	 * @param path
	 * @return null if success, error message or throws an exception if not
	 */
	public String mkDir(String path) throws Exception {
		gotoDir(path, true);
		return null;
	}
	
	/**
	 * Navigate to a directory determined by "path"
	 * Create a new directory in the file structure if it doesn't exist if createIfNotExistent is true
	 * @param path
	 * @return a reference do the DFSFile if success, throws an exception if not
	 */
	public DFSFile gotoDir(String path, boolean createIfNotExistent) throws Exception {
		try {
			if (createIfNotExistent)
				treeWriteLock.lock();
			else
				treeReadLock.lock();
			
			if (path == null)
				throw new Exception("Path cannot be null");
			
			path = sanitizePath(path);
			DFSFile currDFSFile = dfsFileRoot;
			String[] subpath = path.split("/");
			for (int i = 0; i < subpath.length; i++) {
				List<DFSFile> children = currDFSFile.getChildren();
				// check if directory already exists on tree
				int idx = getChildIndex(children, subpath[i], 'D');
				// no directory found, create directory if createIfNotExistent is true, throw Exception otherwise
				if (idx < 0) {
					if (!createIfNotExistent) {
						throw new Exception("Directory doesn't exist");
					}
					DFSFile dfsFile = new DFSFile();
					dfsFile.setName(subpath[i]);
					dfsFile.setType('D');
					// add newly created directory to the tree
					currDFSFile.addChild(dfsFile);
					// refresh idx and children
					children = currDFSFile.getChildren(); 
					idx = getChildIndex(children, subpath[i],'D');
				}
				// change current node to the current sub directory
				currDFSFile = children.get(idx);
			}
			if (createIfNotExistent)
				// success: persist data
				persistFileStructureToDisk();
			return currDFSFile;
		} finally {
			if (createIfNotExistent)
				treeWriteLock.unlock();
			else
				treeReadLock.unlock();
		}
	}
	
	/**
	 * Create a new file in the file structure
	 * @param path
	 * @param filename
	 * @return null if success, errorMessage if not
	 */
	public String touch(String path, String filename) throws Exception {
		if (path == null) throw new Exception("ERROR: Path is null");
		if (filename == null) throw new Exception("ERROR: Path is null");
		
		try {
			treeWriteLock.lock();
			// go to "path" creating parents if they don't exist
			DFSFile dirDFSFile = gotoDir(path, true);
			
			// check if file already exists
			DFSFile dfsFile = dirDFSFile.findChildByNameAndType(filename, 'F');
			// doesn't exist, create it
			if (dfsFile == null) {
				dfsFile = new DFSFile();
				dfsFile.setName(filename);
				dfsFile.setType('F');
				dfsFile.setEmptyFile(true);
				dirDFSFile.addChild(dfsFile);
			} else {
				// already exists, throw exception
				throw new Exception("File/Directory already exists");
			}
			
		} finally {
			treeWriteLock.unlock();
		}
		
		return null;
	}

	/**
	 * Try to get a write lock to a DFSFile.
	 * If the file doesn't exists, create it and lock it in write mode.
	 * 
	 * Returns a reference to the DFSFile
	 */
	public DFSFile aquireWriteLock(String path, String filename, int fileSize) throws Exception {
		if (path == null) throw new Exception("ERROR: Path is null");
		if (filename == null) throw new Exception("ERROR: Filename is null");
		
		try {
			treeWriteLock.lock();
			// go to "path"
			DFSFile dirDFSFile = gotoDir(path, true);
			
			DFSFile dfsFile = dirDFSFile.findChildByNameAndType(filename, 'F');
			// if file doesn't exists, create it
			if (dfsFile == null) {
				dfsFile = new DFSFile();
				dfsFile.setName(filename);
				dfsFile.setType('F');
				dirDFSFile.addChild(dfsFile);
			}
			
			// check if the file has any locks in it
			if (dfsFile.getReadLocks() != null && dfsFile.getReadLocks().size() > 0) {
				throw new FileLockException("File is in use (read mode)");
			}
			if (dfsFile.getWriteLock() != null) {
				throw new FileLockException("File is in use (write mode)");
			}
			
			// lock the file
			DFSLock dfsLock = new DFSLock();
			dfsLock.setAcquiredOn(new Date());
			dfsFile.setWriteLock(dfsLock);
			
			// set/update file size
			dfsFile.setFileSize(fileSize);
			
			// persist changes to disk
			persistFileStructureToDisk();
			
			// return a reference to the file
			return (DFSFile)dfsFile.clone();
		} finally {
			treeWriteLock.unlock();
		}
	}
	
	/**
	 * Try to get a read lock to a DFSFile.
	 * 
	 * Returns a reference to the DFSFile
	 */
	public DFSFile aquireReadLock(String path, String filename, String aquireeId) throws Exception {
		if (path == null) throw new Exception("ERROR: Path is null");
		if (filename == null) throw new Exception("ERROR: Filename is null");
		
		try {
			treeWriteLock.lock();
			// go to "path"
			DFSFile dirDFSFile = gotoDir(path, true);
			
			DFSFile dfsFile = dirDFSFile.findChildByNameAndType(filename, 'F');
			// if file doesn't exists, throw an exception
			if (dfsFile == null) {
				throw new Exception("ERROR: File doesn't exist");
			}
			
			// check if the file has a write lock on it
			if (dfsFile.getWriteLock() != null) {
				throw new FileLockException("File is in use (write mode)");
			}
			
			// lock the file
			DFSLock dfsLock = new DFSLock();
			dfsLock.setAcquiredOn(new Date());
			dfsLock.setAcquiredBy(aquireeId);
			dfsFile.addReadLock(dfsLock);
			
			// persist changes to disk
			persistFileStructureToDisk();
			
			// return a reference to the file
			return (DFSFile)dfsFile.clone();
		} finally {
			treeWriteLock.unlock();
		}
	}
	
	/**
	 * Try to find all datanodes that have an updated version of a certain file
	 * 
	 * Returns null if the file is empty and therefore can be sent to any datanode
	 */
	public List<String> findValidDatanodes(String path, String filename) throws Exception {
		List<String> validDatanodes = new ArrayList<String>();
		try {
			treeReadLock.lock();
			// go to the dir specified by 'path' and find the file
			DFSFile dfsFile = gotoDir(path, false).findChildByNameAndType(filename, 'F');
			// file is empty, meaning it's not persisted in any datanode yet
			if (dfsFile.isEmptyFile()) {
				return null;
			}
			for (String validDatanode : dfsFile.getValidDatanodesId()) {
				validDatanodes.add(validDatanode);
			}
		} finally {
			treeReadLock.unlock();
		}
		return validDatanodes;
	}
	
	/**
	 * Removes all valid datanodes from a dfsFile
	 */
	public void removeValidDatanodes(String path, String filename) throws Exception {
		try {
			treeReadLock.lock();
			// go to the dir specified by 'path' and find the file
			DFSFile dfsFile = gotoDir(path, false).findChildByNameAndType(filename, 'F');
			if (dfsFile.getValidDatanodesId() != null) {
				dfsFile.setPrevValidDatanodesId(dfsFile.getValidDatanodesId());
			}
			dfsFile.setValidDatanodesId(null);
		} finally {
			treeReadLock.unlock();
		}
	}
	
	/**
	 * Perform relevant actions over a file that was written to a datanode, such as:
	 * 	no error found: add datanode to the valid datanodes list
	 * 	no error found and file is empty: unmark empty file flag
	 * 	release write lock
	 */
	public void revalidateWrittenToFile(String filepath, String filename, String datanodeId, boolean noErrorsFound) throws Exception {
		try {
			treeWriteLock.lock();
			// get the file
			DFSFile dfsFile = gotoDir(filepath, false).findChildByNameAndType(filename, 'F');
			
			if (noErrorsFound) {
				Logger.logDebug("Adding datanode " + datanodeId + " as a valid datanode for file " + dfsFile.getName(), getClass());
				dfsFile.addDatanodeToValidList(datanodeId);
				dfsFile.setEmptyFile(false);
			}
			// unlock the file
			dfsFile.setWriteLock(null);
			
			persistFileStructureToDisk();
		} catch (Exception e) {
			System.out.println("ERROR SAVING SUCCESSFUL WRITE OPERATION TO DISK");
			e.printStackTrace();
			throw e;
		} finally {
			treeWriteLock.unlock();
		}
		
	}	
	
	/**
	 * Perform relevant actions over a file that was read from a datanode, such as:
	 * 	release read lock
	 */
	public void revalidateReadFromFile(String filepath, String filename, String datanodeId, boolean noErrorsFound, String lockAquireeId) throws Exception {
		try {
			treeWriteLock.lock();
			// get the file
			DFSFile dfsFile = gotoDir(filepath, false).findChildByNameAndType(filename, 'F');
			
			// unlock the file
			dfsFile.removeReadLock(lockAquireeId);
			
			persistFileStructureToDisk();
		} catch (Exception e) {
			System.out.println("ERROR SAVING SUCCESSFUL WRITE OPERATION TO DISK");
			e.printStackTrace();
			throw e;
		} finally {
			treeWriteLock.unlock();
		}
		
	}	

	private String sanitizePath(String path) {
		if (path.startsWith("/"))
			path = path.substring(1);
		if (path.endsWith("/"))
			path = path.substring(0, path.length()-1);
		return path;
	}
	
	
	private int getChildIndex(List<DFSFile> tree, String path, char type) {
		if (tree == null || tree.isEmpty())
			return -1;
		int idx = 0;
		boolean foundChild = false;
		for (DFSFile child : tree) {
			if (child.getName().equals(path) && child.getType() == type) {
				foundChild = true;
				break;
			}
			idx++;
		}
		return foundChild ? idx : -1;
	}
	
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		DFSFile dfsFile = dfsFileRoot;
		str.append(dfsFile.getName());
		str.append("\n");
		printChild(str, dfsFile, 0);
		return str.toString();
	}
	private void printChild(StringBuilder str, DFSFile currDFSFile, int pad) {
		if (currDFSFile.getChildren() == null)
			return;
		pad+=3;
		char padBuf[] = new char[pad];
		Arrays.fill(padBuf, ' ');
		for (DFSFile child : currDFSFile.getChildren()) {
			str.append(new String(padBuf));
			str.append(child.getName());
			str.append("\n");
			if (child.getChildren() != null && !child.getChildren().isEmpty()) {
				printChild(str, child, pad);
			}
		}
	}

	public DFSFile retrieveFileInfo(String path, String filename) {
		if (path == null || filename == null)
			return null;
		
		try {
			treeWriteLock.lock();
			// go to "path"
			DFSFile dirDFSFile = gotoDir(path, true);
			
			DFSFile dfsFile = dirDFSFile.findChildByNameAndType(filename, 'F');
			// if file doesn't exists, return null
			if (dfsFile == null) {
				return null;
			}
			
			// return a reference to the file
			return (DFSFile)dfsFile.clone();
		} catch (Exception e) {
			Logger.logError("ERROR TRYING TO GET FILE INFO FOR " + path + " / " + filename, e, getClass());
			return null;
		} finally {
			treeWriteLock.unlock();
		}
	}

}
