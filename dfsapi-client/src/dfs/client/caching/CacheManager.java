package dfs.client.caching;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import dfs.client.config.ClientConfigurations;
import dfs.common.logger.Logger;

public class CacheManager {

	
	private static HashMap<CacheKey, CachedFile> caches = new HashMap<CacheKey, CachedFile>();
	
	private static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private static ReadLock readLock = readWriteLock.readLock();
	private static WriteLock writeLock = readWriteLock.writeLock();
	
	/**
	 * Returns file size if available, or -1 if not
	 */
	public static int getFileSize(String path, String filename) {
		readLock.lock();
		try {
			// get the cached file info (if any)
			CachedFile cachedFile = caches.get(new CacheKey(path, filename));
			if (cachedFile != null) {
				return cachedFile.getFileSize();
			} else {
				return -1;
			}
		} finally {
			readLock.unlock();
		}
	}
	
	/**
	 * Returns the file offset of the first cached block starting at 'offset'. -1 if not availabled
	 */
	public static int nextOffsetOnCache(String path, String filename, int offset, int size) {
		readLock.lock();
		try {
			// get the cached file info (if any)
			CachedFile cachedFile = caches.get(new CacheKey(path, filename));
			if (cachedFile != null) {
				return cachedFile.nextOffsetCached(offset, size);
			} else {
				return -1;
			}
		} finally {
			readLock.unlock();
		}
		
	}
	
	/**
	 * Try to read "size" bytes starting at "offset" into "appendTo" starting at "appendOffset"
	 * 
	 */
	public static int readFromCache(String path, String filename, int offset, int size, byte[] appendTo, int appendOffset) {
		int totalBytesRead = 0;
		int blocksRead = 0;
		readLock.lock();
		try {
			// get the file cache info
			CachedFile cachedFile = caches.get(new CacheKey(path, filename));
			// allocate space for a data block to be read from cache
			byte[] blockRead = new byte[ClientConfigurations.CACHE_BLOCK_SIZE];
			while (true) {
				// try to get read next block for current offset
				int bytesRead = cachedFile.readBlock(offset+totalBytesRead, blockRead);
				// check if was able to read something
				if (bytesRead <= 0) break;
				// save read bytes
				for(int i=0; i < bytesRead && i < (size-totalBytesRead); i++) {
					appendTo[appendOffset+totalBytesRead+i] = blockRead[i];
				}
				blocksRead++;
				totalBytesRead += bytesRead;
				if (totalBytesRead >= size) break;
			}
		} finally {
			readLock.unlock();
		}
		Logger.logStats("Local Cache: " + filename + " -- Read " + totalBytesRead + " bytes from " + blocksRead + " blocks", CacheManager.class); 
		if (totalBytesRead >= size) {
			return size;
		} else {
			return totalBytesRead;
		}
	}
	
	
	public static void writeCacheBlock(String path, String filename, int offset, byte[] data) {
		try {
			writeLock.lock();
			
			CacheKey cacheKey = new CacheKey(path, filename);
			CachedFile cachedFile = caches.get(cacheKey);
			// file is not cached yet, cache it
			if (cachedFile == null) {
				cachedFile = new CachedFile(cacheKey);
				caches.put(cachedFile.getCacheKey(), cachedFile);
			}
			
			// cache data
			cachedFile.writeCache(offset, data);
			
		} finally {
			writeLock.unlock();
		}
		
	}

	public static void removeCacheBlock(CachedBlock cachedBlock) {
		// get the cachedBlock reference and remove it
		CachedFile cachedFile = caches.get(cachedBlock.getCachedFile().getCacheKey());
		cachedFile.getCachedBlocks().remove(cachedBlock.getBlockOffset());
		cachedBlock.setContent(null);
		cachedBlock = null;
	}
	
	
	public static int getNumberOfBlocksOnCache() {
		int blocksOnCache = 0;
		for (CachedFile cachedFile : caches.values()) {
			for (CachedBlock cachedBlock : cachedFile.getCachedBlocks().values()) {
				if (cachedBlock.getContent() != null && cachedBlock.getContent().length > 0) {
					blocksOnCache++;
				}
			}
		}
		return blocksOnCache;
	}
	
}
