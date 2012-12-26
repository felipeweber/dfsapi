package dfs.datanode.filesystem;

import java.io.Serializable;

/**
 * An in-memory map of the DFSFiles path+filename for data blocks in disk.
 * This class should not be instantiated, use getInstance() instead
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DatanodeDFSFile implements Serializable, Cloneable {
	
	private static final long serialVersionUID = 1L;

	private String localPath;
	private String localFilename;
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
		DatanodeDFSFile datanodeDFSFile = (DatanodeDFSFile) super.clone();
		datanodeDFSFile.localPath = new String(localPath);
		datanodeDFSFile.localFilename = new String(localFilename);
		return datanodeDFSFile;
	}
	public String getLocalPath() {
		return localPath;
	}
	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}
	public String getLocalFilename() {
		return localFilename;
	}
	public void setLocalFilename(String localFilename) {
		this.localFilename = localFilename;
	}
	
	
	
}
