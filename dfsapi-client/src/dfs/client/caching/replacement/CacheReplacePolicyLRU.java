package dfs.client.caching.replacement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import dfs.client.caching.CacheManager;
import dfs.client.caching.CachedBlock;
import dfs.common.logger.Logger;

/**
 * Implements LRU (Least Recently Used) policy for cache replacement
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CacheReplacePolicyLRU extends CacheReplacePolicy {

	/**
	 * Stores a ordered by insertion reference to cacheBlocks.
	 * Using LinkedHashMap guarantees that the access time to get any reference is always O(1);
	 * It also guarantees that the access time to get the last reference is also O(1)
	 */
	private static HashMap<CachedBlock, Boolean> cachesBlocksReferenceByUpdatedTime = new LinkedHashMap<CachedBlock, Boolean>();
	
	
	@Override
	protected long removeXBytesFromCache(long bytesToRemove, CachedBlock cachedBlockBeingWritten) {
		long bytesRemoved = 0;
		int blocksRemoved = 0;
		Logger.logDebug("CachingPolicy LRU: MAX_CACHE_SIZE, removing oldest cacheBlock(s)", getClass());
		// iterate through cached blocks, removing them based on oldest to newest until
		//	removed the required amount of bytes 
		for (Iterator cachedBlocksEntrySetIterator = cachesBlocksReferenceByUpdatedTime.entrySet().iterator(); cachedBlocksEntrySetIterator.hasNext();) {
			Entry<CachedBlock, Boolean> cacheEntry = (Entry<CachedBlock, Boolean>) cachedBlocksEntrySetIterator.next();
			if (!cacheEntry.getKey().equals(cachedBlockBeingWritten)) {
				Logger.logDebug("CachingPolicy LRU: Removing cachedBlock " + cacheEntry.getKey(), getClass());
				// decrease super.totalBytesCached
				bytesRemoved += cacheEntry.getKey().getContent().length;
				
				// remove from the cache itself
				CacheManager.removeCacheBlock(cacheEntry.getKey());
				
				// remove the cached block reference
				cachedBlocksEntrySetIterator.remove();
				cacheEntry.getKey().setContent(null);
				
				blocksRemoved++;
				// if super.totalBytesCached is now less than MAX_CACHE_SIZE, we're done
				if (bytesRemoved >= bytesToRemove) {
					break;
				}
			}
		}
		Logger.logStats("Local Cache Policy: Removed " + bytesRemoved + " in " + blocksRemoved + " blocks", getClass());
		return bytesRemoved;
	}
	
	@Override
	protected void writeToCache(CachedBlock cachedBlock) {
		Logger.logDebug("CachingPolicy LRU: Start caching block " + cachedBlock, getClass());
		
		// add a reference to the cacheBlock at the end of the list
		cachesBlocksReferenceByUpdatedTime.put(cachedBlock, Boolean.TRUE);
	}

	@Override
	public void readFromCache(CachedBlock cachedBlock) {
		Logger.logDebug("CachingPolicy LRU: Accessed cache block " + cachedBlock, getClass());
		
		// remove block from list
		cachesBlocksReferenceByUpdatedTime.remove(cachedBlock);
		// add to end of the list
		cachesBlocksReferenceByUpdatedTime.put(cachedBlock, Boolean.TRUE);
	}
	
}