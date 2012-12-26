package dfs.datanode.filesystem.manager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.util.HashMap;

import dfs.common.logger.Logger;
import dfs.datanode.comm.replication.send.ReplicationPipelineSender;
import dfs.datanode.filesystem.DatanodeDFSFile;
import dfs.datanode.filesystem.FileSystemImpl;
import dfs.datanode.filesystem.FileSystemInterface;

/**
 * An object that manages all current operations over files,
 * 	as well as holds all currently opened files.
 * This class cannot be instantiated and should be called using getInstance()
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileSystemManager {

	// Map by fileGlobalKey
	// Holds an instance of RandomAccessFile for each file currently being written to
	private static HashMap<BigInteger, RandomAccessFile> fileAccesses = new HashMap<BigInteger, RandomAccessFile>();
	
	private static final FileSystemManager INSTANCE = new FileSystemManager();
	private FileSystemManager() { };
	public static FileSystemManager getInstance() {
		return INSTANCE;
	}
	
	/**
	 * Perform the operations necessary to start a data block write operation,
	 * 	such as verifying if the file already exists, allocating the space to it,
	 * 	create a in memory reference to the File...
	 * 
	 * @param fileGlobalKey
	 */
	public void startDataBlockWrite(BigInteger fileGlobalKey, int fileSize) throws Exception {
		Logger.logInfo("Called startDataBlockWrite for file " + fileGlobalKey);
		RandomAccessFile fileAccess;
		
		FileSystemInterface fileSystem = FileSystemImpl.getInstance();
		DatanodeDFSFile datanodeDFSFile = fileSystem.findFileByFileKey(fileGlobalKey);
		// local file doesn't exist
		if (datanodeDFSFile == null) {
			// create the file in the virtual filesystem
			datanodeDFSFile = fileSystem.createFile(fileGlobalKey);
		}

		// check if a file access for that file already exists
		synchronized (fileAccesses) {
			fileAccess = fileAccesses.get(fileGlobalKey);
			// don't exist yet, create one and add to the list
			//	get a RandomAccessFile to access it
			if (fileAccess == null) {
				fileAccess = fileSystem.accessDataBlock(fileGlobalKey, true);
				fileAccesses.put(fileGlobalKey, fileAccess);
			}
		}
		synchronized (fileAccess) {
			fileAccess.setLength(fileSize);
		}
		
		// TODO backup the old file in case the operation fails?
		
	}
	
	/**
	 * Perform the operations necessary when writing to a file block.
	 *  
	 * @param fileGlobalKey
	 * @param byteRead
	 * @param actualRead
	 */
	public void writeBlock(BigInteger fileGlobalKey, byte[] byteRead, int offset, int actualRead) throws IOException {
		RandomAccessFile accessFile;
		synchronized (fileAccesses) {
			accessFile = fileAccesses.get(fileGlobalKey);
		}
		accessFile.seek(offset);
		accessFile.write(byteRead, 0, actualRead);
	}
	
	/** 
	 * Perform the operations necessary to end a file write operation,
	 * 	such as persisting the temporary file and closing the output stream.
	 * Also start sending this data block to the next datanode in the replication pipeline
	 * 
	 * @param fileGlobalKey
	 */
	public void endDataBlockWrite(Long operationId, BigInteger fileGlobalKey, int totalFileSize) {
		Logger.logInfo("Called endDataBlockWrite for file " + fileGlobalKey);
		synchronized (fileAccesses) {
			// close the RandomAccessFile
			RandomAccessFile accessFile = fileAccesses.get(fileGlobalKey);
			try {
				// TODO had to comment it out as it caused problems when a client requests multiple reads in different threads to the same file (i.e. in the caching process of the client) 
				/*accessFile.close();
				fileAccesses.remove(fileGlobalKey);*/
			} catch (Exception e) {
				Logger.logError("ERROR CLOSING DATA BLOCK RANDOMACCESFILE OBJECT", getClass());
			}
		}
		
		// TODO persist the temporary file
		
		// Initialize process to communicate with the next datanode in the replication pipeline
		ReplicationPipelineSender replicationPipelineSender = ReplicationPipelineSender.getInstance();
		replicationPipelineSender.initializeReplicationPipeline(operationId, fileGlobalKey, totalFileSize);

	}
	
	
	/**
	 * Perform the operations necessary to start a data block read operation,
	 * 	such as verifying if the file exists and create a in memory reference to the File
	 * 
	 * @param fileGlobalKey
	 * @return the file size
	 */
	public int startDataBlockRead(BigInteger fileGlobalKey) throws Exception {
		return startDataBlockRead(fileGlobalKey, 0, 0);
	}
	
	/**
	 * Perform the operations necessary to start a data block read operation,
	 * 	such as verifying if the file exists and create a in memory reference to the File
	 * 
	 * @param fileGlobalKey
	 * @return the file size
	 */
	public int startDataBlockRead(BigInteger fileGlobalKey, int offset, int size) throws Exception {
		Logger.logInfo("Called startDataBlockRead for file " + fileGlobalKey);
		RandomAccessFile fileAccess;
		
		FileSystemInterface fileSystem = FileSystemImpl.getInstance();
		DatanodeDFSFile datanodeDFSFile = fileSystem.findFileByFileKey(fileGlobalKey);
		// local file doesn't exist
		if (datanodeDFSFile == null) {
			throw new Exception("ERROR: File with global key " + fileGlobalKey + " doesn't exist");
		}
		
		// check if a file access for that file already exists
		synchronized (fileAccesses) {
			fileAccess = fileAccesses.get(fileGlobalKey);
			// don't exist yet, create one and add to the list
			//	get a RandomAccessFile to access it
			if (fileAccess == null) {
				fileAccess = fileSystem.accessDataBlock(fileGlobalKey, false);
				fileAccesses.put(fileGlobalKey, fileAccess);
			}
		}
		
		if (offset == 0 && size == 0) {
			return (int)fileAccess.length();
		} else {
			if (offset+size > (int)fileAccess.length()) {
				return (int)fileAccess.length() - offset;
			} else {
				return size;
			}
		}
	}
	
	
	/**
	 * Read bytes from a data block into byteRead returning the amount of data actually
	 * read or -1 if finished reading
	 *  
	 * @param fileGlobalKey
	 * @param byteRead
	 * @param actualRead
	 */
	public int readBlock(byte[] byteRead, BigInteger fileGlobalKey, int offset, int noBytesToRead) throws IOException {
		RandomAccessFile accessFile;
		synchronized (fileAccesses) {
			accessFile = fileAccesses.get(fileGlobalKey);
		}
		accessFile.seek(offset);
		return accessFile.read(byteRead, 0, noBytesToRead);
	}
	
	/** 
	 * Perform the operations necessary to end a file read operation,
	 * 	such as closing the RandomAccessFile to it.
	 * 
	 * @param fileGlobalKey
	 */
	public void endDataBlockRead(BigInteger fileGlobalKey) {
		Logger.logInfo("Called endDataBlockRead for file " + fileGlobalKey);
		synchronized (fileAccesses) {
			// close the RandomAccessFile
			RandomAccessFile accessFile = fileAccesses.get(fileGlobalKey);
			try {
				// TODO had to comment it out as it caused problems when a client requests multiple reads in different threads to the same file (i.e. in the caching process of the client)
				/*accessFile.close();
				fileAccesses.remove(fileGlobalKey);*/
			} catch (Exception e) {
				Logger.logError("ERROR CLOSING DATA BLOCK RANDOMACCESFILE OBJECT FOR FILE GLOBAL KEY " + fileGlobalKey, getClass());
			}
		}
	}
	
	
}
