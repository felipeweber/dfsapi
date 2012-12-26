package dfs.client.comm.datanode.listener;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import dfs.client.comm.datanode.protocol.FileOperationDatanodeProtocolHandler;
import dfs.client.comm.datanode.protocol.impl.FileOperationDatanodeProtocolHandlerImpl;

public class FileOperationHandler extends Thread {
	
	private Socket s;
	
	public FileOperationHandler(Socket s) {
		this.s = s;
	}
	
	public void run() {
		try {
			DataInputStream in = new DataInputStream(s.getInputStream());
			DataOutputStream out = new DataOutputStream(s.getOutputStream());
			
			// pass the input and output streams to the protocol handler,
			//  which effectively implements the relevant protocol
			FileOperationDatanodeProtocolHandler protocolHandler = new FileOperationDatanodeProtocolHandlerImpl(in, out);
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
