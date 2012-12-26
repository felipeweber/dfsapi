package dfs.controller.comm.datanode.listener;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import dfs.controller.comm.datanode.protocol.FileOperationResProtocolHandler;
import dfs.controller.comm.datanode.protocol.impl.FileOperationResProtocolHandlerImpl;

public class DatanodeCommHandler extends Thread {
	
	private Socket s;
	
	public DatanodeCommHandler(Socket s) {
		this.s = s;
	}
	
	public void run() {
		try {
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			
			// pass the input and output streams to the protocol handler,
			//  which effectively implements the relevant protocol
			FileOperationResProtocolHandler protocolHandler = new FileOperationResProtocolHandlerImpl(in, out);
			protocolHandler.startProtocol();
			
			in.close();
			out.close();
			
		} catch (Exception e) {
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
