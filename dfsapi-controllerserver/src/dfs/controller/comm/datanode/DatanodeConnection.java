package dfs.controller.comm.datanode;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import dfs.common.configparser.DatanodeInfo;
import dfs.common.logger.Logger;
import dfs.common.protocol.configurations.InternalConnectionType;

public class DatanodeConnection extends Socket {

	/**
	 * Info (id, host, startPort) of the datanode we're connecting to
	 */
	private DatanodeInfo datanodeInfo;
	
	/**
	 * Purpose of the connection
	 * Must be a valid InternalConnectionType
	 */
	private String internalConnectionType;
	
	public DatanodeConnection(DatanodeInfo datanodeInfo, String internalConnectionType) throws Exception {
		this.datanodeInfo = datanodeInfo;
		this.internalConnectionType = internalConnectionType;
	}
	
	public DatanodeConnection(DatanodeInfo datanodeInfo, int operationType) throws Exception {
		this.internalConnectionType = InternalConnectionType.findConnectionTypeByOperation(operationType);
		this.datanodeInfo = datanodeInfo;
	}
	
	/**
	 * Tries to open this connection
	 * 
	 * Returns true if connection succeeds false if it doesn't
	 * 
	 * @return boolean
	 */
	public boolean connectOrReconnect() {
		// if not connected yet or if connection was closed, (re)connect
		try {
			int startPort = datanodeInfo.getStartPort();
			int sumPort = InternalConnectionType.getSumPort(internalConnectionType);
			int port = startPort + sumPort;
			String host = datanodeInfo.getHost();
			
			SocketAddress sa = new InetSocketAddress(host, port);
			connect(sa);
			Logger.logDebug("Connecting to " + datanodeInfo.getId() + " on port " + port, getClass());
			Logger.logDebug("Using local port: " + getLocalPort(), getClass());
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
}
