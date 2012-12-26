package dfs.datanode.comm.datanode.listener;

import java.net.ServerSocket;
import java.net.Socket;

import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.datanode.DatanodeConfigurations;
import dfs.datanode.comm.replication.ThreadPoolManager;

/** 
 * Waits for requests from datanodes to receive files over the Replication Pipeline 
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 */
public class ReplicationPipelineCommListener extends Thread {

	public void run() {
		try {
			int port = DatanodeConfigurations.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.DATANODES_REPLICATION_PIPELINE_OPERATIONS);
			Logger.logDebug("Started listening for Replication Pipeline operations on port " + port, getClass());
			ServerSocket ss = new ServerSocket(port);
			while (true) {
				try {
					/* Listens indefinitely and passes connections to the Handler */
					Socket s = ss.accept();
					ThreadPoolManager threadPoolManager = ThreadPoolManager.getInstance();
					threadPoolManager.addThread(new ReplicationPipelineCommHandler(s));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
