package dfs.datanode.comm.controller.listener;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import dfs.common.logger.Logger;
import dfs.datanode.comm.controller.protocol.FileOperationReqProtocolHandler;
import dfs.datanode.comm.controller.protocol.impl.FileOperationReqProtocolHandlerImpl;

/**
 * Handle a request from a client to perform a given file operation
 * and responds accordingly
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class FilesOperationCommHandler extends Thread {

	private Socket s;
	
	public FilesOperationCommHandler(Socket s) {
		this.s = s;
	}
	
	public void run() {
		try {
			Logger.logDebug("Controller connected", this.getClass());
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			
			// pass the input and output streams to the protocol handler,
			//  which effectively implements the relevant protocol
			FileOperationReqProtocolHandler protocolHandler = new FileOperationReqProtocolHandlerImpl(in, out);
			protocolHandler.startProtocol();
			
			in.close();
			out.close();
			Logger.logDebug("Controller disconnected", this.getClass());
		} catch (IOException e) {
			System.out.println("ERROR: Connection to client lost");
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
