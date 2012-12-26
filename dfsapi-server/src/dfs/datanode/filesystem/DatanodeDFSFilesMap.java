package dfs.datanode.filesystem;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;

/**
 * Holds a map of the files stored by this datanode
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DatanodeDFSFilesMap implements Serializable {

	private static final long serialVersionUID = 1L;

	// map key is the fileKey, as defined by the controller
	private HashMap<BigInteger, DatanodeDFSFile> datanodeDFSFilesMap;

	public DatanodeDFSFilesMap() {
		datanodeDFSFilesMap = new HashMap<BigInteger, DatanodeDFSFile>();
	}

	public HashMap<BigInteger, DatanodeDFSFile> getDatanodeDFSFilesMap() {
		return datanodeDFSFilesMap;
	}
	public void setDatanodeDFSFilesMap(
			HashMap<BigInteger, DatanodeDFSFile> datanodeDFSFilesMap) {
		this.datanodeDFSFilesMap = datanodeDFSFilesMap;
	}
	
	
}
