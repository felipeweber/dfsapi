package dfs.controller.comm.state.parameters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import dfs.common.configparser.DatanodeInfo;
import dfs.controller.filestructure.DFSFile;

/**
 * Class used as a parameter in the Operation object for  File Read operation
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileReadOperationParameter implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private DFSFile dfsFile;
	private String filePath;
	private int size;
	private int offset;
	// holds a map of datanodes that will be receiving this file, and if they already received it
	private HashMap<DatanodeInfo, Boolean> datanodes = new HashMap<DatanodeInfo, Boolean>();
		
	public FileReadOperationParameter(DFSFile dfsFile, String path) {
		this.dfsFile = dfsFile;
		this.filePath = path;
	}
	
	public FileReadOperationParameter(DFSFile dfsFile, String filePath, int offset, int size) {
		super();
		this.dfsFile = dfsFile;
		this.filePath = filePath;
		this.size = size;
		this.offset = offset;
	}

	/**
	 * Returns a datanode that has not been marked to send the file yet
	 * and mark it
	 * 	or null if all datanodes are already marked to send this file
	 */
	public DatanodeInfo findNextDatanode() {
		Set<Entry<DatanodeInfo, Boolean>> entrySet = datanodes.entrySet();
		for (Entry<DatanodeInfo, Boolean> entry : entrySet) {
			if (!entry.getValue() || entry.getValue().equals(Boolean.FALSE)) {
				entry.setValue(Boolean.TRUE);
				return entry.getKey();
			}
		}
		return null;
	}
	
	public void addDatanode(DatanodeInfo datanode) {
		this.datanodes.put(datanode, Boolean.FALSE);
	}
	
	public DFSFile getDfsFile() {
		return dfsFile;
	}
	public void setDfsFile(DFSFile dfsFile) {
		this.dfsFile = dfsFile;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
}
