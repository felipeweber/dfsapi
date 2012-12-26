package dfs.client.caching.replacement;

import dfs.client.caching.CachedBlock;
import dfs.client.config.ClientConfigurations;
import dfs.common.logger.Logger;

/**
 * Defines the methods that a Cache Replacement Policy must implement, mainly
 * 	removeXBytesFromCache, writeToCache and readFromCache.
 * The Class used as an implementation to this interface is defined by ClientConfigurations.CACHE_REPLACEMENT_POLICY_CLASS
 * The implementation of this interface should have a reference to the CachedBlocks (passed by parameter),
 * 	and call the CacheManager.remove(CacheBlock) method to remove cache blocks
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public abstract class CacheReplacePolicy {

	private static CacheReplacePolicy INSTANCE = null;
	public static CacheReplacePolicy getInstance() {
		if (INSTANCE == null) {
			// we can't construct the class in the INSTANCE field declaration because Class.newInstance() can throw exceptions;
			// because of that, there is a very remote (very, VERY remote) possibility of entering this "if" statement twice
			// due to kernel preemption, so we "synchronize" an inner "if" just in case
			synchronized (CacheReplacePolicy.class) {
				if (INSTANCE == null) {
					try {
						INSTANCE = ((Class<? extends CacheReplacePolicy>) ClientConfigurations.CACHE_REPLACE_POLICY_CLASS).newInstance();
					} catch (Exception e) {
						Logger.logError("CRITICAL ERROR: CACHE POLICY UNDEFINED, COULDN'T INSTANTIATE POLICY CLASS" + ClientConfigurations.CACHE_REPLACE_POLICY_CLASS, e, CacheReplacePolicy.class);
						e.printStackTrace();
					}
				}
			}
		}
		return INSTANCE;
	}
	protected CacheReplacePolicy() {
	}
	
	private long totalBytesCached = 0;
	
	public void writeToCache(int sizeToCache, CachedBlock cachedBlock) {
		// cachedBlock may or may not already have content cached (in case of overwriting);
		//	check what's the actual size of caching that will be added (or even subtracted)
		int cachingActualSize = 0;
		if (cachedBlock.getContent() != null) {
			cachingActualSize = sizeToCache - cachedBlock.getContent().length;
		} else {
			cachingActualSize = sizeToCache;
		}
		// increase (or even decrease) the amount of total bytes cached
		totalBytesCached += cachingActualSize;
		
		// check if the amount of bytes to be cached requires bytes to be removed from cache
		if (totalBytesCached > ClientConfigurations.MAX_CACHE_SIZE) {
			totalBytesCached -= removeXBytesFromCache(totalBytesCached - ClientConfigurations.MAX_CACHE_SIZE, cachedBlock);
		}
		
		// Call the ReplacementPolicy implementation of writeToCache
		writeToCache(cachedBlock);
	
	}
	
	/**
	 * The implementation of this method must remove at least X bytes from the Cache,
	 * 	by calling the method CacheManager.remove(CachedBlock).
	 * It MUST NOT remove the cachedBlockBeingWritten cache block
	 * It must also return the number of bytes that were actually removed
	 */
	protected abstract long removeXBytesFromCache(long bytesToRemove, CachedBlock cachedBlockBeingWritten);

	/**
	 * This method is called to indicate that a given cachedBlock has been written to the cache.
	 * The implementation of this method should simply add the cachedBlock to it's internal	controls
	 */
	protected abstract void writeToCache(CachedBlock cachedBlock);
	
	/**
	 * This method is called to indicate that a given cachedBlock has been read from the cache
	 * The implementation of this method should perform the internal operations required when 
	 * 	a cache read is done in a particular cachedBlock (for example: update last accessed date, used times, etc)
	 */
	public abstract void readFromCache(CachedBlock cachedBlock);
		
}
