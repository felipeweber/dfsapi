package tests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Receiver {

	private static int MAX_READ_BLOCK_SIZE = 64*1024;
	
	public static void main(String args[]) {	
		try {
			ServerSocket ss = new ServerSocket(10000);
			Socket s = ss.accept();
			
			InputStream in = s.getInputStream();
			
			FileOutputStream fos = new FileOutputStream(new File("/home/felipe/teste2.txt"));
			
			byte[] byteRead = new byte[MAX_READ_BLOCK_SIZE];
			
			int actualRead;

			// we trust that the sender will only send what we need to read
			while(true) {
				actualRead = in.read(byteRead, 0, byteRead.length);
				if (actualRead == -1) break;
				System.out.println(actualRead);
				fos.write(byteRead, 0, actualRead);
				
			}

			fos.close();
			in.close();
			s.close();
			ss.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
