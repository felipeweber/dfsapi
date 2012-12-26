package dfs.client.api;

import dfs.client.comm.datanode.listener.FileOperationListener;

/**
 * Methods to control the client application
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DFSClient {

	public boolean startListeners() {
		try {
			Thread responseListener = new FileOperationListener();
			responseListener.start();
		} catch (Exception e) {
			System.out.println("ERROR: Cannot start file operation response listener");
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
}
