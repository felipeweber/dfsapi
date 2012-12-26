package dfs.controller.comm.datanode.listener;

import java.net.ServerSocket;
import java.net.Socket;

import dfs.common.protocol.configurations.InternalConnectionType;
import dfs.controller.ControllerInfo;

public class DatanodeCommListener extends Thread {

	public void run() {
		try {
			ServerSocket ss = new ServerSocket(ControllerInfo.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE));
			System.out.println("CONTROLLER: Listening on port " + ControllerInfo.START_PORT + InternalConnectionType.getSumPort(InternalConnectionType.CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE));
			while (true) {
				try {
					/* Listens indefinitely and passes connections to the Handler */
					Socket s = ss.accept();
					new DatanodeCommHandler(s).start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
