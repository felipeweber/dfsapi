package dfs.datanode.comm.datanode;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;

public class DatanodeConnection extends Socket {

	private String datanodeId;
	private String datanodeHost;
	private int connectionPort;
	
	public DatanodeConnection(DatanodeInfo datanodeInfo, String internalConnectionType) throws Exception {
		this.datanodeId = datanodeInfo.getId();
		this.datanodeHost = datanodeInfo.getHost();
		connectionPort = datanodeInfo.getStartPort() + InternalConnectionType.getSumPort(internalConnectionType);
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
			SocketAddress sa = new InetSocketAddress(datanodeHost, connectionPort);
			connect(sa);
			Logger.logDebug("Using local port: " + getLocalPort(), getClass());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}


	
}
