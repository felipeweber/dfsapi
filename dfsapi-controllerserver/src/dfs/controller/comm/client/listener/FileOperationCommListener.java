package dfs.controller.comm.client.listener;

import java.net.ServerSocket;
import java.net.Socket;

import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.controller.ControllerInfo;

public class FileOperationCommListener extends Thread {

	public void run() {
		try {
			ServerSocket ss = new ServerSocket(ControllerInfo.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.FILE_OPERATIONS_INTERN_TYPE));
			System.out.println("CONTROLLER: Listening on port " + ControllerInfo.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.FILE_OPERATIONS_INTERN_TYPE));
			while (true) {
				try {
					/* Listens indefinitely and passes connections to the Handler */
					Socket s = ss.accept();
					new FileOperationCommHandler(s).start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
