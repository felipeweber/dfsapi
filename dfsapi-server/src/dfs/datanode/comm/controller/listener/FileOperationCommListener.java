package dfs.datanode.comm.controller.listener;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.datanode.DatanodeConfigurations;

/** 
 * Waits for requests from the controller to perform operations over files
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 */
public class FileOperationCommListener extends Thread {

	public void run() {
		ServerSocket ss = null;
		try {
			int port = DatanodeConfigurations.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.FILE_OPERATIONS_INTERN_TYPE);
			Logger.logDebug("Started listening for File Operations on port " + port, getClass());
			ss = new ServerSocket(port);
			while (true) {
				try {
					/* Listens indefinitely and passes connections to the Handler */
					Socket s = null;
					s = ss.accept();
					Logger.logDebug("Connection received", getClass());
					new FilesOperationCommHandler(s).start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				ss.close();
			} catch (IOException e) {
				Logger.logError("Error closing SocketServer", e, getClass());
				e.printStackTrace();
			}
		}
	}
	
}
