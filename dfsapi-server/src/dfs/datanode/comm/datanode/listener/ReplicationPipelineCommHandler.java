package dfs.datanode.comm.datanode.listener;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import dfs.common.logger.Logger;
import dfs.datanode.comm.datanode.protocol.ReplicationPipelineProtocolHandlerImpl;

/**
 * Handle a request from a Datanode from the Replication Pipeline
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ReplicationPipelineCommHandler extends Thread {

	private Socket s;
	
	public ReplicationPipelineCommHandler(Socket s) {
		this.s = s;
	}
	
	public void run() {
		try {
			Logger.logDebug("Datanode connected in the Replication Pipeline", this.getClass());
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			
			// pass the input and output streams to the protocol handler,
			//  which effectively implements the relevant protocol
			ReplicationPipelineProtocolHandlerImpl protocolHandler = new ReplicationPipelineProtocolHandlerImpl(in, out);
			protocolHandler.startProtocol();
			
			in.close();
			out.close();
			Logger.logDebug("Datanode disconnected", this.getClass());
		} catch (IOException e) {
			System.out.println("ERROR: Connection to datanode lost");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				s.close();
			} catch (IOException e) {
				System.out.println("Error closing connection " + s);
				e.printStackTrace();
			}
		}
	}
	
}
