package dfs.datanode;

import dfs.common.configparser.DatanodeInfoParser;
import dfs.common.exceptions.DatanodeInfoException;
import dfs.common.logger.Logger;
import dfs.datanode.comm.controller.listener.FileOperationCommListener;
import dfs.datanode.comm.datanode.listener.ReplicationPipelineCommListener;

public class DatanodeStarter {

	/**
	 * Main method, starts all listener threads from the datanode
	 * 
	 * @arg0: datanode name. If not specified, use default 'datanode1'
	 * 	The datanode name should be depicted on the configuration file (common.conf)
	 * 	with its respective configurations
	 * 
	 */
	public static void main(String args[]) {
		
		String myName = "datanode1";
		if (args != null && args.length > 0 && args[0] != null && !args[0].isEmpty())
			myName = args[0];
		
		try {
			// get its own metadata from common.conf
			String[] infoTmp = DatanodeInfoParser.parseDatanodeInfo(myName);
			DatanodeConfigurations.ID = infoTmp[0];
			DatanodeConfigurations.IP = infoTmp[1];
			DatanodeConfigurations.START_PORT = Integer.parseInt(infoTmp[2]);
			DatanodeConfigurations.ROOT_DIR = infoTmp[3];
			
			Logger.logDebug("Starting datanode " + myName, DatanodeStarter.class);
			
			Thread fileOperationsListener = new FileOperationCommListener();
			fileOperationsListener.start();
			
			Thread replicationPipelineListener = new ReplicationPipelineCommListener();
			replicationPipelineListener.start();
			
		} catch (DatanodeInfoException d) {
			System.out.println("ERROR: " + d.getMessage());
			d.printStackTrace();
		}
		
		
	}

}
