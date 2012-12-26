package dfs.controller.comm.state.parameters;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import dfs.common.configparser.DatanodeInfo;
import dfs.controller.filestructure.DFSFile;

/**
 * Class used as a parameter in the Operation object for  File Write operation
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FileWriteOperationParameter implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private DFSFile dfsFile;
	private String filePath;
	private int totalWriteSize;
	// holds a map of datanodes that will be receiving this file, and if they already received it
	private HashMap<DatanodeInfo, Boolean> datanodes = new HashMap<DatanodeInfo, Boolean>();
	
	public FileWriteOperationParameter(DFSFile dfsFile, String path, int totalWriteSize) {
		this.dfsFile = dfsFile;
		this.filePath = path;
		this.totalWriteSize = totalWriteSize;
	}
	
	/**
	 * Returns a datanode that has not been marked to receive the file yet
	 * and mark it
	 * 	or null if all datanodes are already marked to recieve this file
	 */
	public DatanodeInfo findNextDatanode() {
		Set<Entry<DatanodeInfo, Boolean>> entrySet = datanodes.entrySet();
		for (Entry<DatanodeInfo, Boolean> entry : entrySet) {
			if (!entry.getValue()) {
				entry.setValue(Boolean.TRUE);
				return entry.getKey();
			}
		}
		return null;
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
	public int getTotalWriteSize() {
		return totalWriteSize;
	}
	public void setTotalWriteSize(int totalWriteSize) {
		this.totalWriteSize = totalWriteSize;
	}
	public void addDatanode(DatanodeInfo datanode) {
		this.datanodes.put(datanode, Boolean.FALSE);
	}
	
}
