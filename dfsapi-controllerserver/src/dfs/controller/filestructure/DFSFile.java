package dfs.controller.filestructure;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DFSFile implements Comparable<DFSFile>, Cloneable {
	
	/**
	 * Sequential unique key that identifies each file
	 */
	private BigInteger fileKey;
	private static BigInteger maxFileKey = BigInteger.ONE;
	/**
	 * 'F' for file or 'D' for directory
	 */
	private char type;

	private String name;
	
	// File extra properties
	private int fileSize;
	
	// Children (subdirectories, files)
	private List<DFSFile> children;

	private boolean emptyFile;
	
	// File locks
	private List<DFSLock> readLocks;
	private DFSLock writeLock;
	
	// a list of datanodes that have an updated version of that file
	private List<String> validDatanodesId;
	private List<String> prevValidDatanodesId;

	public DFSFile() {
		fileKey = new BigInteger(maxFileKey.toString());
		maxFileKey = maxFileKey.add(BigInteger.ONE);
	}
	
	public void addChild(DFSFile dfsFile) throws Exception {
		if (children == null) {
			children = new ArrayList<DFSFile>();
		} else {
			// children is not null, verify if child with same name and type already exists
			for (DFSFile child : children) {
				if (child.getName().equals(dfsFile.getName()) && child.getType() == dfsFile.getType()) {
					throw new Exception("File/Directory already exists");
				}
			}
		}
		children.add(dfsFile);
	}
	
	public boolean childExists(String filename, char type) {
		if (children == null)
			return false;
		// children is not null, verify if child with same name and type already exists
		for (DFSFile child : children) {
			if (child.getName().equals(filename) && child.getType() == type) {
				return true;
			}
		}
		return false;
	}
	
	public DFSFile findChildByNameAndType(String name, char type) {
		if (children == null) {
			return null;
		}
		for (DFSFile child : children) {
			if (child.getName().equals(name) && child.getType() == type) {
				return child;
			}
		}
		return null;
	}

	public void emptyValidaDatanodesList() {
		this.validDatanodesId = new ArrayList<String>();
	}
	
	public void addDatanodeToValidList(String datanodeId) {
		if (this.validDatanodesId == null)
			this.validDatanodesId = new ArrayList<String>();
		validDatanodesId.add(datanodeId);
	}
	
	public void addReadLock(DFSLock dfsLock) {
		if (this.readLocks == null) {
			this.readLocks = new ArrayList<DFSLock>();
		}
		readLocks.add(dfsLock);
	}
	
	public void removeReadLock(String lockAquireeId) {
		for (Iterator<DFSLock> readLocksIter = this.readLocks.iterator(); readLocksIter.hasNext();) {
			DFSLock readLock = (DFSLock) readLocksIter.next();
			if (readLock.getAcquiredBy().equals(lockAquireeId)) {
				readLocksIter.remove();
			}
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof String) {
			return this.name.equals((String)obj);
		} else if (obj instanceof DFSFile) {
			return this.name.equals(((DFSFile)obj).name);
		} else {
			return super.equals(obj);
		}
	}

	@Override
	public int compareTo(DFSFile o) {
		return this.name.compareTo(o.name);
	}

	@Override
	public Object clone() {
		try {
			DFSFile dfsFile = (DFSFile)super.clone();
			if (children != null)
				dfsFile.children = new ArrayList<DFSFile>(children);
			dfsFile.emptyFile  = emptyFile;
			dfsFile.name = new String(name);
			dfsFile.type  = type;
			dfsFile.fileKey = new BigInteger(fileKey.toString());
			return dfsFile;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 'F' for file or 'D' for directory
	 */
	public char getType() {
		return type;
	}
	/**
	 * 'F' for file or 'D' for directory
	 */
	public void setType(char type) {
		this.type = type;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<DFSLock> getReadLocks() {
		return readLocks;
	}
	public void setReadLocks(List<DFSLock> readLocks) {
		this.readLocks = readLocks;
	}
	public DFSLock getWriteLock() {
		return writeLock;
	}
	public void setWriteLock(DFSLock writeLock) {
		this.writeLock = writeLock;
	}
	/**
	 * DO NOT use this reference to change the structure, use addChildren() and removeChildren() instead
	 * @return reference to this node's children
	 */
	public List<DFSFile> getChildren() {
		return children;
	}
	public void setChildren(List<DFSFile> children) {
		this.children = children;
	}
	public List<String> getValidDatanodesId() {
		return validDatanodesId;
	}
	public void setValidDatanodesId(List<String> validDatanodesId) {
		this.validDatanodesId = validDatanodesId;
	}

	public boolean isEmptyFile() {
		return emptyFile;
	}

	public void setEmptyFile(boolean emptyFile) {
		this.emptyFile = emptyFile;
	}
	public BigInteger getFileKey() {
		return fileKey;
	}
	public int getFileSize() {
		return fileSize;
	}
	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}

	public List<String> getPrevValidDatanodesId() {
		return prevValidDatanodesId;
	}

	public void setPrevValidDatanodesId(List<String> prevValidDatanodesId) {
		this.prevValidDatanodesId = prevValidDatanodesId;
	}

}
