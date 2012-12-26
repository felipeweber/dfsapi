package dfs.client.comm.controller;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import dfs.common.configparser.ControllerInfoParser;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;

/**
 * Stores the controller connection
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ControllerConnection extends Socket {

	// basic attributes to connect
	public static String host;
	public static int port;
	
	public ControllerConnection() throws Exception {
		if (host == null || host.length() == 0) {
			String[] ret = ControllerInfoParser.parseControllerServerInfo();
			host = ret[0];
			port = Integer.parseInt(ret[1]) + InternalConnectionType.getSumPort(InternalConnectionType.FILE_OPERATIONS_INTERN_TYPE);
		}
	}
	
	/**
	 * Tries to open this connection
	 * If already connected, check if the connection is active and
	 * reconnect if needed.
	 * 
	 * Returns true if connection succeeds false if it doesn't
	 * 
	 * @return boolean
	 */
	public boolean connectOrReconnectToControllerServer() {
		// check if a connection exists
		if (!isClosed() && isConnected()) {
			return true;
		}
		// if not connected yet or if connection was closed, (re)connect
		Logger.logDebug("Connection: Connecting to Controller on port " + port, getClass());
		try {
			SocketAddress sa = new InetSocketAddress(host, port);
			connect(sa);
			Logger.logDebug("Using local port: " + getLocalPort(), getClass());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

}
