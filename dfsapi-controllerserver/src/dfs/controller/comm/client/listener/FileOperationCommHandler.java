package dfs.controller.comm.client.listener;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import dfs.common.logger.Logger;
import dfs.controller.comm.client.protocol.FileOperationReqProtocolHandler;
import dfs.controller.comm.client.protocol.impl.FileOperationReqProtocolHandlerImpl;

public class FileOperationCommHandler extends Thread {
	
	private Socket s;
	
	public FileOperationCommHandler(Socket s) {
		this.s = s;
	}
	
	public void run() {
		try {
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			
			// pass the input and output streams to the protocol handler,
			//  which effectively implements the relevant protocol
			FileOperationReqProtocolHandler protocolHandler = new FileOperationReqProtocolHandlerImpl(in, out);
			protocolHandler.startProtocol();
			
			in.close();
			out.close();
			
		} catch (Exception e) {
			// TODO Error while communicating with the datanode, handle error by reverting stuff and trying again instead of throwing an exception, which is not catched anyway because we are in a THREAD!
			Logger.logError("ERROR ON FileOperationCommHandler", e, getClass());
			e.printStackTrace();
		} finally {
			try {
				s.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
}
