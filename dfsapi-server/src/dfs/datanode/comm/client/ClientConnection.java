package dfs.datanode.comm.client;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import dfs.common.logger.Logger;

public class ClientConnection extends Socket {

	private int clientId;
	private String clientHost;
	private int clientPort;
	
	public ClientConnection(int clientId, String clientHost, int clientPort) {
		this.clientId = clientId;
		this.clientHost = clientHost;
		this.clientPort = clientPort;
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
	public boolean connectOrReconnect() {
		// check if a connection exists
		if (!isClosed() && isConnected()) {
			return true;
		}
		// if not connected yet or if connection was closed, (re)connect
		try {
			SocketAddress sa = new InetSocketAddress(clientHost, clientPort);
			connect(sa);
			Logger.logDebug("Using local port: " + getLocalPort(), getClass());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
}
