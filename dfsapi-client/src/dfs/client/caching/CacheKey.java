package dfs.client.caching;

public class CacheKey {

	private String filePath;
	private String fileName;
	
	public CacheKey(String filePath, String fileName) {
		this.filePath = filePath;
		this.fileName = fileName;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof CacheKey) {
			CacheKey compareTo = (CacheKey) obj;
			return this.filePath.equals(compareTo.filePath) && this.fileName.equals(compareTo.fileName);
		}
		return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return (filePath + fileName).hashCode();
	}

	@Override
	public String toString() {
		return filePath + "/" + fileName;
	}
	
	
	
	
}
