package dfs.client.caching;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;

import dfs.client.caching.replacement.CacheReplacePolicy;
import dfs.client.config.ClientConfigurations;
import dfs.common.logger.Logger;

public class CachedFile {

	private CacheKey cacheKey;
	
	private int fileSize;
	
	// cached data blocks indexed by their overall file offset
	private TreeMap<Integer, CachedBlock> cachedBlocks = new TreeMap<Integer, CachedBlock>();
	
	public CachedFile(CacheKey cacheKey) {
		this.cacheKey = cacheKey;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof CachedFile && ((CachedFile)obj).getCacheKey() != null) {
			return ((CachedFile)obj).getCacheKey().equals(this.cacheKey);
		} else if (obj != null && obj instanceof CacheKey) {
			return ((CacheKey)obj).equals(this.cacheKey);
		} else {
			return super.equals(obj);
		}
	}

	@Override
	public int hashCode() {
		return this.cacheKey.hashCode();
	}

	/**
	 * Returns the file offset of the first cached block starting at 'offset', or -1 if not available
	 */
	public int nextOffsetCached(int neededOffset, int size) {
		// if cachedBlocks is empty don't even bother checking
		if (cachedBlocks == null || cachedBlocks.isEmpty()) {
			return -1;
		}
		int currCheckingOffset = neededOffset;
		while (currCheckingOffset < neededOffset+size) {
			// try to find a cache for the current offset
			Entry<Integer, CachedBlock> cachedBlockEntry = cachedBlocks.floorEntry(currCheckingOffset);
			
			// cacheBlockEntry null in this case means the current offset is smaller than the first cache available
			if (cachedBlockEntry == null) {
				// set current checking offset to the first available offset and continue,
				//	verifying in the "while" clause if it's applicable to this request
				currCheckingOffset = cachedBlocks.firstKey();
				continue;
			}
			
			Integer cachedBlockedOffset = cachedBlockEntry.getKey();
			CachedBlock cachedBlock = cachedBlockEntry.getValue();
			// currCheckingOffset is cached in the current block
			if (currCheckingOffset < (cachedBlockedOffset + cachedBlock.getContent().length)) {
				return currCheckingOffset;
			}
			// got to the last block
			if (cachedBlockedOffset.equals(cachedBlocks.lastKey())) {
				break;
			}
			
			// currCheckingOffset is not cached in the current block, try to find the next one
			currCheckingOffset = cachedBlockedOffset + cachedBlock.getContent().length + 1;
		}
		
		// if got down here means couldn't find any offset that's cached
		return -1;
	}
	
	/**
	 * Insert "data" into the file cache blocks
	 * Returns the number of bytes cached
	 */
	public long writeCache(int startOffset, byte[] data) {
		// cache blocks according to ClientConfigurations.CACHE_BLOCK_SIZE
		int bytesAlreadyCachedSize = 0;
		int blocksWritten = 0;
		while(bytesAlreadyCachedSize < data.length) {
			
			// check if a block for that offset is already cached
			CachedBlock cachedBlock = cachedBlocks.get(startOffset+bytesAlreadyCachedSize);
			// if a block for that offset is not cached yet, create it
			if (cachedBlock == null) {
				Integer blockOffset = startOffset+bytesAlreadyCachedSize;
				cachedBlock = new CachedBlock(this, blockOffset);
				cachedBlocks.put(cachedBlock.getBlockOffset(), cachedBlock);
			}
			
			// the last part of "data" might be smaller than the actual cache block size
			int sizeToCache;
			if (bytesAlreadyCachedSize + ClientConfigurations.CACHE_BLOCK_SIZE > data.length) {
				sizeToCache = data.length - bytesAlreadyCachedSize;
			} else {
				sizeToCache = ClientConfigurations.CACHE_BLOCK_SIZE;
			}
			
			// Call the control method for the Cache Replacement Policy
			CacheReplacePolicy.getInstance().writeToCache(sizeToCache, cachedBlock);
			
			cachedBlock.setContent(Arrays.copyOfRange(data, bytesAlreadyCachedSize, bytesAlreadyCachedSize+sizeToCache));
			
			bytesAlreadyCachedSize += sizeToCache;
			blocksWritten++;
		}
		Logger.logStats("Local Cache: Written " + bytesAlreadyCachedSize + " bytes to " + blocksWritten + " blocks", getClass());
		return bytesAlreadyCachedSize;
	}
	
	/**
	 * Read cache block referent to "offset" into b.
	 * Returns the number of bytes read.
	 */
	public int readBlock(int offset, byte[] b) {
		// find the cached block that can have "offset" cached on it
		Entry<Integer, CachedBlock> cachedBlockEntry = cachedBlocks.floorEntry(offset);
		Integer firstCachedBlockedOffset = cachedBlockEntry.getKey();
		CachedBlock cachedBlock = cachedBlockEntry.getValue();
		if (cachedBlock == null) {
			return 0;
		}
		int bytesRead = 0;
		for (int i = offset - firstCachedBlockedOffset; i < cachedBlock.getContent().length; i++) {
			b[bytesRead] = cachedBlock.getContent()[i];
			bytesRead++;
		}
		if (bytesRead > 0) {
			// Call the control method for the Cache Replacement Policy
			CacheReplacePolicy.getInstance().readFromCache(cachedBlock);
		}
		return bytesRead;
	}

	@Override
	public String toString() {
		return cacheKey.toString();
	}
	
	public TreeMap<Integer, CachedBlock> getCachedBlocks() {
		return cachedBlocks;
	}

	public void setCachedBlocks(TreeMap<Integer, CachedBlock> cachedBlocks) {
		this.cachedBlocks = cachedBlocks;
	}

	public int getFileSize() {
		return fileSize;
	}

	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}

	public CacheKey getCacheKey() {
		return cacheKey;
	}

	public void setCacheKey(CacheKey cacheKey) {
		this.cacheKey = cacheKey;
	}

}
