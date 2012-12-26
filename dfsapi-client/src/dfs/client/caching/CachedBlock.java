package dfs.client.caching;


public class CachedBlock {

	private CachedFile cachedFile;
	private Integer blockOffset;
	
	private byte[] content;
	
	public CachedBlock(CachedFile cachedFile, Integer blockOffset) {
		this.cachedFile = cachedFile;
		this.blockOffset = blockOffset;
		
		this.content = null;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof CachedBlock) {
			CachedBlock compareTo = (CachedBlock) obj;
			if (compareTo.getCachedFile() != null && compareTo.getBlockOffset() != null) {
				return compareTo.getCachedFile().equals(this.cachedFile) && compareTo.getBlockOffset().equals(this.blockOffset);
			}
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return (String.valueOf(this.cachedFile.hashCode()) + String.valueOf(this.blockOffset)).hashCode();
	}

	@Override
	public String toString() {
		return cachedFile.toString() + " , at offset " + blockOffset;
	}
	
	public byte[] getContent() {
		return this.content;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
	public CachedFile getCachedFile() {
		return cachedFile;
	}
	public void setCachedFile(CachedFile cachedFile) {
		this.cachedFile = cachedFile;
	}

	public Integer getBlockOffset() {
		return blockOffset;
	}

	public void setBlockOffset(Integer blockOffset) {
		this.blockOffset = blockOffset;
	}
	
}
