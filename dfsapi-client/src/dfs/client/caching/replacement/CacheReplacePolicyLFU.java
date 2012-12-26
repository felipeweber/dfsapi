package dfs.client.caching.replacement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import dfs.client.caching.CacheManager;
import dfs.client.caching.CachedBlock;
import dfs.common.logger.Logger;

/**
 * Implements LFU (Least Frequently Used) policy for cache replacement
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CacheReplacePolicyLFU extends CacheReplacePolicy {

	/**
	 * Stores a reference to a list of cachedBlocks ordered by the number of times accessed.
	 * Using a TreeMap guarantees that the access time to get a reference is in the best case scenario O(log(n)).
	 */
	private static TreeMap<Long, List<CachedBlock>> cachesBlocksReferenceByUseFrequency = new TreeMap<Long, List<CachedBlock>>();
	/**
	 * Store the last updated time of each CachedBlock, facilitating and improving performance in the task
	 * 	of finding the actual block in the cachesBlocksReferenceByUseFrequency TreeMap
	 */
	private static HashMap<CachedBlock, Long> cachesBlocksReference = new HashMap<CachedBlock, Long>();
	
	@Override
	protected long removeXBytesFromCache(long bytesToRemove, CachedBlock cachedBlockBeingWritten) {
		long bytesRemoved = 0;
		int blocksRemoved = 0;
		Logger.logDebug("CachingPolicy LFU: MAX_CACHE_SIZE has been achieved, removing least used cacheBlock(s) for " + bytesToRemove + " bytes", getClass());
		// iterate through cached blocks, removing them based on least used
		//	until totalBytesCached is less than MAX_CACHE_SIZE
		for (Iterator cachedBlocksEntrySetIterator = cachesBlocksReferenceByUseFrequency.entrySet().iterator(); cachedBlocksEntrySetIterator.hasNext();) {
			Entry<Long, List<CachedBlock>> cacheEntry = (Entry<Long, List<CachedBlock>>) cachedBlocksEntrySetIterator.next();
			for (Iterator cachedBlocksIterator = cacheEntry.getValue().iterator(); cachedBlocksIterator
					.hasNext();) {
				CachedBlock cachedBlock = (CachedBlock) cachedBlocksIterator.next();
				if (!cachedBlock.equals(cachedBlockBeingWritten)) {
					Logger.logDebug("CachingPolicy LFU: Removing cachedBlock " + cachedBlock, getClass());
					// decrease super.totalBytesCached
					bytesRemoved += cachedBlock.getContent().length;
					// remove the cached block reference
					cachedBlocksIterator.remove();
					cachesBlocksReference.remove(cachedBlock);
					// remove from the cache itself
					CacheManager.removeCacheBlock(cachedBlock);
					blocksRemoved++;
					// if removed the amount of bytes required
					if (bytesRemoved >= bytesToRemove) {
						break;
					}
				}
			}
			// if the list of cachedBlock for a given used frequency is now empty, remove the entry itself
			if (cacheEntry.getValue().size() == 0) {
				cachedBlocksEntrySetIterator.remove();
			}
			// if removed the amount of bytes required
			if (bytesRemoved >= bytesToRemove) {
				break;
			}
		}
		Logger.logStats("Local Cache Policy: Removed " + bytesRemoved + " in " + blocksRemoved + " blocks", getClass());
		return bytesRemoved;
	}
	
	@Override
	protected void writeToCache(CachedBlock cachedBlock) {
		Logger.logDebug("CachingPolicy LFU: Start caching block " + cachedBlock, getClass());
		// add a reference to the newly added cacheBlock or update if already existent
		Long existentCacheUsedFrequency = cachesBlocksReference.get(cachedBlock);
		if (existentCacheUsedFrequency != null) {
			// remove from the map by frequency
			List<CachedBlock> existentCachedBlocks = cachesBlocksReferenceByUseFrequency.get(existentCacheUsedFrequency);
			for (Iterator cachedBlocksIterator = existentCachedBlocks.iterator(); cachedBlocksIterator
					.hasNext();) {
				CachedBlock existingCachedBlock = (CachedBlock) cachedBlocksIterator.next();
				if (existingCachedBlock.equals(cachedBlock)) {
					cachedBlocksIterator.remove();
					break;
				}
			}
			// if the list of cachedBlock for a given used frequency is now empty, remove the entry itself
			if (existentCachedBlocks.size() == 0) {
				cachesBlocksReferenceByUseFrequency.remove(existentCacheUsedFrequency);
			}
		}
		
		// update/add references to the newly updated/added cacheBlock
		cachesBlocksReference.put(cachedBlock, new Long(1));
		// if cache for used n times already exists
		List<CachedBlock> existingCachedBlockList = cachesBlocksReferenceByUseFrequency.get(new Long(1));
		if (existingCachedBlockList != null) {
			existingCachedBlockList.add(cachedBlock);
		} else {
			ArrayList<CachedBlock> cachedBlocks = new ArrayList<CachedBlock>();
			cachedBlocks.add(cachedBlock);
			cachesBlocksReferenceByUseFrequency.put(new Long(1), cachedBlocks);
		}
	}

	
	@Override
	public void readFromCache(CachedBlock cachedBlock) {
		// get the existing cacheBlock previous accessed times
		Long usedFrequency = cachesBlocksReference.get(cachedBlock);
		// remove from the map by frequency
		List<CachedBlock> existentCachedBlocks = cachesBlocksReferenceByUseFrequency.get(usedFrequency);
		for (Iterator cachedBlocksIterator = existentCachedBlocks.iterator(); cachedBlocksIterator
				.hasNext();) {
			CachedBlock existingCachedBlock = (CachedBlock) cachedBlocksIterator.next();
			if (existingCachedBlock.equals(cachedBlock)) {
				cachedBlocksIterator.remove();
				break;
			}
		}
		// if the list of cachedBlock for a given frequency is now empty, remove the entry itself
		if (existentCachedBlocks.size() == 0) {
			cachesBlocksReferenceByUseFrequency.remove(usedFrequency);
		}
		
		// update references to the cacheBlock
		usedFrequency++;
		cachesBlocksReference.put(cachedBlock, usedFrequency);
		// check if an entry for this used frequency already exists
		List<CachedBlock> existingCachedBlockList = cachesBlocksReferenceByUseFrequency.get(usedFrequency);
		if (existingCachedBlockList != null) {
			existingCachedBlockList.add(cachedBlock);
		} else {
			ArrayList<CachedBlock> cachedBlocks = new ArrayList<CachedBlock>();
			cachedBlocks.add(cachedBlock);
			cachesBlocksReferenceByUseFrequency.put(usedFrequency, cachedBlocks);
		}
	}
	
}
