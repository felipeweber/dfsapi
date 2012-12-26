package dfs.client.fileoperations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import dfs.client.caching.CacheManager;
import dfs.client.comm.controller.protocol.FileOperationReqProtocol;
import dfs.client.comm.controller.protocol.impl.FileOperationReqProtocolImpl;
import dfs.client.comm.fileoperations.FileOperationsReadReqThread;
import dfs.client.comm.fileoperations.FileOperationsReqThreadPool;
import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.FileInfoReqProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;

/**
 * Manages file operations requests, verifying operations made over cache
 * 	or sending requisitions over the network.
 * We count that the cache is always up to date, as the Controller must send a message
 * 	in case a given file is not longer valid of if a file has been locked in write mode
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileOperationImpl {

	public static byte[] read(String path, String filename, int offset, int size) throws Exception {
		// List of all threads requesting reads from the DFS 
		List<Future<FileOperationsReadReqThread>> runningThreadsFuture = new ArrayList<Future<FileOperationsReadReqThread>>();
		List<FileOperationsReadReqThread> runningThreads = new ArrayList<FileOperationsReadReqThread>();
		
		// request basic file informations if not already available
		int fileSize = CacheManager.getFileSize(path, filename);
		if (fileSize <= 0) {
			// no thread in this case, as this information is crucial for the rest of the process to work
			fileSize = retrieveFileInfo(path, filename);
		}
		
		// size = 0 means reading whole file
		if (size == 0) {
			size = fileSize;
		}
		
		byte[] contentResponse = new byte[size];
		// go from "offset" to "offset+size" checking if a certain block is available in the cache or 
		//	if it need to be requested to the DFS
		int nextOffsetOnCache = -1;
		int currMaxSize = size;
		int currOffset = offset;
		while(currOffset < (offset + size)) {
			// controls how much is left to read
			currMaxSize = size - (currOffset - offset);
			
			// check if the current offset can be read from cache; if not, will return the next offset that can
			nextOffsetOnCache = CacheManager.nextOffsetOnCache(path, filename, currOffset, currMaxSize);
			
			// if haven't found a match for the current offset, request a read from the DFS
			if (!(nextOffsetOnCache == currOffset)) {
				int reqReadSize;
				// no more reads from cache
				if (nextOffsetOnCache == -1) {
					reqReadSize = currMaxSize;
				} else {
					// nextOffsetOnCache != -1 - there's still data that can be read from cache
					reqReadSize = nextOffsetOnCache - currOffset;
				}
				// add thread to request reads to the thread pool
				FileOperationsReadReqThread readReq = new FileOperationsReadReqThread(path, filename, currOffset, reqReadSize);
				FileOperationsReqThreadPool threadPool = FileOperationsReqThreadPool.getInstance();
				runningThreadsFuture.add(threadPool.addThread(readReq));
				runningThreads.add(readReq);
			}
			
			// found a match on the cache, read from it
			if (nextOffsetOnCache > -1) {
				currOffset = nextOffsetOnCache;
				currMaxSize = size - (currOffset - offset);
				// append relevant content to contentResponse[offset-currOffset] and return
				//	how many bytes were appended
				int bytesRead = CacheManager.readFromCache(path, filename, nextOffsetOnCache, currMaxSize, contentResponse, nextOffsetOnCache-offset);
				
				currOffset += bytesRead;
				// if read all requested bytes, break and just wait for all read requests from DFS to return
				if (currOffset >= offset+size) break;
				
			} else {
				// no more matches on the cache: break and just wait for all read requests from DFS to return
				break;
			}
		}
		
		// Waits for all running threads (if any) to complete.
		// Add the content response from the DFS to the response;
		// Add the content response from the DFS to the local cache
		int currThread=0;
		for (Future<FileOperationsReadReqThread> future : runningThreadsFuture) {
			// wait for thread to complete
			future.get();
			FileOperationsReadReqThread readReqThread =  runningThreads.get(currThread);
			
			// save the content into contentResponse and into the local cache
			for(int i=0; i < readReqThread.getSize() && i < readReqThread.getContentResponse().length; i++) {
				contentResponse[readReqThread.getOffset() - offset + i] = readReqThread.getContentResponse()[i];
			}
			CacheManager.writeCacheBlock(path, filename, readReqThread.getOffset(), readReqThread.getContentResponse());
			
			currThread++;
		}
		Logger.logStats("File Request: read " + contentResponse.length + " bytes", FileOperationImpl.class);
		return contentResponse;
		
	}

	public static int retrieveFileInfo(String path, String filename) {
		FileInfoReqProto.Builder fileInfoReqProto = FileInfoReqProto.newBuilder();
		
		FilePathProto.Builder filePathProto = FilePathProto.newBuilder();
		filePathProto.setPath(path);
		
		fileInfoReqProto.setPath(filePathProto);
		fileInfoReqProto.setFilename(filename);
		
		FileOperationReqProtocol fileOperationProtocol = new FileOperationReqProtocolImpl();
		int fileSize;
		try {
			fileSize = fileOperationProtocol.retrieveFileInfo(fileInfoReqProto.build());
		} catch (Exception e) {
			Logger.logError("ERROR TRYING TO GET FILE INFO FOR " + path + " / " + filename, e, FileOperationImpl.class);
			return -1;
		}
		
		return fileSize;
	}
	
}
