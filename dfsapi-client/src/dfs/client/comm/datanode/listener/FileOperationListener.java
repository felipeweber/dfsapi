package dfs.client.comm.datanode.listener;

import java.net.ServerSocket;
import java.net.Socket;

import dfs.client.config.ClientConfigurations;

public class FileOperationListener extends Thread {

	public void run() {
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(ClientConfigurations.FILE_OP_RES_LISTEN_PORT);
			while (true) {
				try {
					/* Listens indefinitely and passes connections to the Handler */
					Socket s = ss.accept();
					new FileOperationHandler(s).start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				ss.close();
			} catch (Exception e) {
			}
		}
	}
	
}
