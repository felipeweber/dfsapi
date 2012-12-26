package dfs.controller.comm.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.configparser.DatanodeInfoParser;
import dfs.common.exceptions.DatanodeInfoException;
import dfs.controller.filestructure.DFSFile;

public class Datanodes {

	// Number of datanodes that will have a copy of a file
	public static int REPLICATION_FACTOR = 3;
	
	/**
	 * Hold information on datanodes, by id
	 */
	private static HashMap<String, DatanodeInfo> datanodes = new HashMap<String, DatanodeInfo>();
	
	public static void updateDatanodesInfo() {
		synchronized (datanodes) {
			try {
				// get the datanodes information from the config file
				List<String[]> datanodesInfoConfParsed = DatanodeInfoParser.parseDatanodesInfo();
				
				// go through datanodes from config file and fill the list
				for (String[] tokens : datanodesInfoConfParsed) {
					String id = tokens[0];
					int startPort = Integer.parseInt(tokens[2]);
					String host = tokens[1];
					
					DatanodeInfo datanodeInfo = new DatanodeInfo(id, host, startPort);
					
					datanodes.put(id, datanodeInfo);
				}
				
			} catch (DatanodeInfoException e) {
				System.out.println("ERROR: Error parsing the configuration file. Aborted");
				System.out.println("ERROR: " + e.getCause());
			}
		}
	}
	
	/**
	 * Find a a list of all datanodes that will be receiving a given file
	 * 
	 */
	public static List<DatanodeInfo> findDatanodesForFileWrite(DFSFile dfsFile) {
		// TODO actually try to find the best datanodes

		List<DatanodeInfo> possibleDatanodes = new ArrayList<DatanodeInfo>();
		
		// if dfsFile already has some validDatanodes, try to use it
		if (dfsFile != null && dfsFile.getValidDatanodesId() != null) {
			for(String datanodeId : dfsFile.getValidDatanodesId()) {
				if (!possibleDatanodes.contains(datanodeId)) {
					synchronized (datanodes) {
						possibleDatanodes.add(datanodes.get(datanodeId));
						if (possibleDatanodes.size() >= REPLICATION_FACTOR) {
							return possibleDatanodes;
						}
					}
				}
			}
		} 
		
		// don't have enough datanodes yet
		synchronized (datanodes) {
			Set<Entry<String, DatanodeInfo>> entrySet = datanodes.entrySet();
			for (Entry<String, DatanodeInfo> entry : entrySet) {
				if (!possibleDatanodes.contains(entry.getValue())) {
					possibleDatanodes.add(entry.getValue());
					if (possibleDatanodes.size() >= REPLICATION_FACTOR) {
						return possibleDatanodes;
					}
				}
			}
		}
		// return datanodes
		return possibleDatanodes;
	}
	
	/**
	 * Find a a list of valid datanodes that contain a given file
	 * 
	 */
	public static List<DatanodeInfo> findDatanodesForFileRead(DFSFile dfsFile) {
		// TODO actually try to find the best datanodes

		List<DatanodeInfo> possibleDatanodes = null;
		
		// use validDatanodes from the DFSFile
		if (dfsFile != null && dfsFile.getValidDatanodesId() != null) {
			for(String datanodeId : dfsFile.getValidDatanodesId()) {
				if (possibleDatanodes == null || !possibleDatanodes.contains(datanodeId)) {
					synchronized (datanodes) {
						if (possibleDatanodes == null) {
							possibleDatanodes = new ArrayList<DatanodeInfo>();
						}
						possibleDatanodes.add(datanodes.get(datanodeId));
					}
				}
			}
		} 
		
		// don't have any datanodes
		// TODO No valid datanodes to send a file to a client, treat it
		
		// return datanodes
		return possibleDatanodes;
	}
	
	/**
	 * Find the preferred datanode to connect to.
	 * If sent null for valid datanodeIds, consider any datanode can be used
	 * 
	 * @param datanodeIds
	 * @return datanodeInfo
	 */
	/*public static DatanodeInfo findPreferredDatanode(DFSFile dfsFile) {
		List<String> dontConsiderList = null;
		return findPreferredDatanode(dfsFile, dontConsiderList);
	}*/
	
	/**
	 * Find a preferred datanode to connect to, which is not the "dontConsider"
	 * 
	 * @param datanodeIds
	 * @return datanodeInfo
	 */
	/*public static DatanodeInfo findPreferredDatanode(DFSFile dfsFile, String dontConsider) {
		List<String> dontConsiderList = null;
		if (dontConsider != null) {
			dontConsiderList = new ArrayList<String>();
			dontConsiderList.add(dontConsider);
		}
		
		return findPreferredDatanode(dfsFile, dontConsiderList);
	}*/
	
	/**
	 * Find a preferred datanode to connect to, which is not one of the
	 * 	datanodes not to be considered (for whatever reason)
	 * 
	 * @param datanodeIds
	 * @return datanodeInfo
	 */
	/*public static DatanodeInfo findPreferredDatanode(DFSFile dfsFile, List<String> dontConsiderList) {
		// TODO actually try to find the best datanode

		// if dfsFile already has some validDatanodes, try to use it
		//if (dfsFile != null && dfsFile.getValidDatanodesId() != null) {
			//for(String datanodeId : dfsFile.getValidDatanodesId()) {
				//if (!dontConsiderList.contains(datanodeId)) {
					//synchronized (datanodes) {
						//return datanodes.get(dfsFile.getValidDatanodesId().get(0));
					//}
//				}
	//		}
		//} 
		// dfsFile is null or
		//	dfsFile don't have any validDatanodes yet or 
		//	we couldn't find a validDatanode that can be considered
		 
		synchronized (datanodes) {
			Set<Entry<String, DatanodeInfo>> entrySet = datanodes.entrySet();
			for (Entry<String, DatanodeInfo> entry : entrySet) {
				if (dontConsiderList == null || !dontConsiderList.contains(entry.getValue())) {
					return entry.getValue();
				}
			}
		}
		// couldn't find any datanode
		return null;
	}*/
	
	public static DatanodeInfo getDatanodeInfo(String id) {
		synchronized (datanodes) {
			return datanodes.get(id);
		}
	}
	public static void addDatanode(String id, DatanodeInfo datanodeInfo) {
		synchronized (datanodes) {
			datanodes.put(id, datanodeInfo);
		}
	}
	
}
