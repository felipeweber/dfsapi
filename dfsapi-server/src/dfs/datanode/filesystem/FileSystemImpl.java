package dfs.datanode.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoLsProto;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.datanode.DatanodeConfigurations;

/**
 * Implements a basic virtual file system based on the UNIX file structure Root
 * dir is / and path separator is / Always reads and writes a whole file at a
 * time
 * 
 * @author Felipe Weber
 * 
 */
public class FileSystemImpl implements FileSystemInterface {
	
	private static final FileSystemImpl INSTANCE = new FileSystemImpl();
	// list of files stored by this datanode
	private DatanodeDFSFilesMap datanodeDFSFilesMap;
		
	/* Operations over the filesystem config file must be read/write synchronized:
	   May have multiple read operations at the same time, but writes are exclusive */
	private ReentrantReadWriteLock fileLock = new ReentrantReadWriteLock();
	private ReadLock fileReadLock = fileLock.readLock();
	private WriteLock fileWriteLock = fileLock.writeLock();
	
	/* Operations over the in memory map must be read/write synchronized:
	   May have multiple read operations at the same time, but writes are exclusive */
	private ReentrantReadWriteLock inmemFileSystemLock = new ReentrantReadWriteLock();
	private ReadLock inmemFileSystemReadLock = inmemFileSystemLock.readLock();
	private WriteLock inmemFileSystemWriteLock = inmemFileSystemLock.writeLock();
	
	/** This class cannot be instantiated. Use getInstance() instead.
	 *  This constructor will be called only once while the Controller is running.
	 *  Opens the filesystem file.
	 *  Build the current File System from this datanode from a filesystem.json file on disk
	 * @throws Exception 
	 **/
	private FileSystemImpl() {
		try {
			loadFileStructureFromDisk();
		} catch (Exception e) {
			Logger.logError("CRITICAL ERROR: COULD NOT LOAD FILE STRUCTURE FORM DISK", e, getClass());
		}
	}
	
	public static FileSystemImpl getInstance() throws Exception {
		return INSTANCE;
	}

	/**
	 * Load the DFS File Structure from the Disk (a JSON file)
	 * @throws Exception 
	 */
	private void loadFileStructureFromDisk() throws Exception  {
		try {
			fileReadLock.lock();
			ObjectMapper mapper = new ObjectMapper();
			File f = new File("filesystem.json");
			// if the filesystem.json doesn't exists yet, just initialized the List
			if (!f.exists() || f.length() == 0) {
				datanodeDFSFilesMap = new DatanodeDFSFilesMap();
			} else {
				datanodeDFSFilesMap = mapper.readValue(f, DatanodeDFSFilesMap.class);
			}
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
			mapper.writeValue(new File("filesystem.json"), datanodeDFSFilesMap);
		} finally {
			fileWriteLock.unlock();
		}
	}

	/**
	 * Find a (reference to a) local file based on it's global key
	 */
	public DatanodeDFSFile findFileByFileKey(BigInteger fileKey) {
		try {
			inmemFileSystemReadLock.lock();
			DatanodeDFSFile datanodeDFSFile = datanodeDFSFilesMap.getDatanodeDFSFilesMap().get(fileKey);
			if (datanodeDFSFile == null)
				return null;
			return (DatanodeDFSFile)datanodeDFSFile.clone();
		} catch (CloneNotSupportedException c) {
			c.printStackTrace();
			return null;
		} finally {
			inmemFileSystemReadLock.unlock();
		}
	}
	
	/**
	 * Create a new empty file for the given global fileKey
	 * Simple implementation: use the fileKey as a string and create
	 * the file in the root dir where the datanode is mounted
	 */
	public DatanodeDFSFile createFile(BigInteger fileKey) {
		try {
			inmemFileSystemWriteLock.lock();
			DatanodeDFSFile datanodeDFSFile = new DatanodeDFSFile();
			datanodeDFSFile.setLocalFilename(fileKey.toString()+".blk");
			datanodeDFSFile.setLocalPath("/");
			datanodeDFSFilesMap.getDatanodeDFSFilesMap().put(fileKey, datanodeDFSFile);
			try {
				persistFileStructureToDisk();
			} catch (Exception e) {
				Logger.logError("ERROR PERSISTING FILE STRUCTURE TO DISK", e, getClass());
			}
			try {
				return (DatanodeDFSFile) datanodeDFSFile.clone();
			} catch (CloneNotSupportedException e) {
				e.printStackTrace();
				return null;
			}
		} finally {
			inmemFileSystemWriteLock.unlock();
		}
	}
	
	
	/**
	 * Load a RandomAccessFile to access a local data block,
	 * and create a new data block if in write mode
	 */
	public RandomAccessFile accessDataBlock(BigInteger fileGlobalKey, boolean writeMode) throws IOException {
		File f = new File(DatanodeConfigurations.ROOT_DIR + fileGlobalKey + ".blk");
		RandomAccessFile raf;
		// file don't exist
		if (!f.exists()) {
			// if in write mode, create it
			if (writeMode) {
				f.createNewFile();
				// persist the new structure (we created a file!) to the disk
				try {
					persistFileStructureToDisk();
				} catch (Exception e) {
					Logger.logError("ERROR PERSISTING FILE STRUCTURE TO DISK", e, getClass());
				}
			} else {
				// if in read mode, it should exist
				throw new IOException("ERROR: Data Block for file with global key " + fileGlobalKey + " doesn't exist in the local filesystem");
			}
		}
		try {
			// open 
			raf = new RandomAccessFile(f, "rw");
		} catch (IOException e) {
			System.out.println("ERROR LOADING RandomAccessFile for file global key " + fileGlobalKey);
			e.printStackTrace();
			throw e;
		}
		return raf;
	}
	
	
	/************** OLD STUFF ****************/
	
	
	/**
	 * Navigate from DatanodeInfo.ROOT_DIR to path (Operating System
	 * independent)
	 */
	private File goToPath(String path) throws FileNotFoundException {
		// load ROOT_DIR file
		File rootDir = new File(DatanodeConfigurations.ROOT_DIR);
		
		// separate path based on UNIX-like structure
		String[] directories = path.split("/");
		File currDir = rootDir;
		// iterate through path
		for (String directory : directories) {
			if (directory.isEmpty()) continue;
			boolean fileNotFound = true;
			// try to find currenty directory in file structure
			for(File currFile : currDir.listFiles()) {
				if (currFile.getName().equalsIgnoreCase(directory)) {
					currDir = currFile;
					fileNotFound = false;
					break;
				}
			}
			if (fileNotFound) {
				throw new FileNotFoundException();
			}
		}
		
		return currDir;
	}

	public FileInfoLsProto ls(String path) {
		// check if the path exists
		try {
			// try to open the path
			File file = goToPath(path);
			
			// protocol object to be returned
			FileInfoLsProto.Builder fileInfoLs = FileInfoLsProto.newBuilder();
			FilePathProto.Builder filePath = FilePathProto.newBuilder();
			fileInfoLs.setStartingpath(filePath.setPath("/").build());

			// goes through the folders/files in path
			for (File f : file.listFiles()) {
				FileInfoProto.Builder fip = FileInfoProto.newBuilder();
				fip.setFilename(f.getName());
				fip.setPath(filePath.setPath(f.getPath()).build());
				//fip.setSize(f.length());
				fip.setType(f.isDirectory() ? "D" : "F");

				fileInfoLs.addFile(fip);
			}

			return fileInfoLs.build();

		} catch (FileNotFoundException e) {
			// TODO FILE NOT FOUND: return error in the protocol
			e.printStackTrace();
		}

		return null;
	}

}
