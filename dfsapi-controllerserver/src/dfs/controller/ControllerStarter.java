package dfs.controller;

import dfs.common.configparser.ControllerInfoParser;
import dfs.controller.comm.client.listener.FileOperationCommListener;
import dfs.controller.comm.datanode.listener.DatanodeCommListener;
import dfs.controller.comm.state.Datanodes;

public class ControllerStarter {

	/**
	 * Main method, starts all listener threads from the controller
	 * 
	 */
	public static void main(String args[]) {
		try {
			// get metadata from common.conf
			String[] infoTmp = ControllerInfoParser.parseControllerServerInfo();
			ControllerInfo.IP = infoTmp[0];
			ControllerInfo.START_PORT = Integer.parseInt(infoTmp[1]);
			
			// Starts Operations listener (handles connections from the clients) 
			Thread operationsRequestListener = new FileOperationCommListener();
			operationsRequestListener.start();
			
			// Starts Datanode Responses listener
			Thread datanodeCommListener = new DatanodeCommListener();
			datanodeCommListener.start();
			
			// Get datanodes info
			Datanodes.updateDatanodesInfo();
		
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
			e.printStackTrace();
		}
		
		
	}

}
