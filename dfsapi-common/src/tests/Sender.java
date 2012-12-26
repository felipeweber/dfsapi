package tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Calendar;
import java.util.Date;

public class Sender {

	private static int NETWORK_BLOCK_SIZE = 64 * 1024;
	
	public static void main(String args[]) {
		try {
			SocketAddress sa = new InetSocketAddress("127.0.0.1", 10000);
			Socket s = new Socket();
			s.connect(sa);
			
			File f = new File("/home/felipe/teste.txt");
			
			OutputStream out = s.getOutputStream();

			long fileSize = f.length();
			
			// send file in blocks of NETWORK_BLOCK_SIZE
			FileInputStream fis = new FileInputStream(f);
			byte[] byteRead = new byte[NETWORK_BLOCK_SIZE];


			Date start = new Date();
			Calendar cStart = Calendar.getInstance();
			cStart.setTime(start);
			long totalSent=0;
			long actuallySend;
			while(fis.read(byteRead) != -1) {
				
				totalSent+=byteRead.length;
				
				// OutputStream always sends byteRead.length bytes,
				//	so we need to control it to send only what's necessary
				//	in the last write
				if (totalSent>fileSize)
					actuallySend = fileSize-totalSent+byteRead.length;
				else
					actuallySend = byteRead.length;
								
				out.write(byteRead, 0, (int)actuallySend);
				out.flush();
			}
			// flush the last bytes
			
			Date end = new Date();
			Calendar cEnd = Calendar.getInstance();
			cEnd.setTime(end);
			
			System.out.println("Elapsed: " + (cEnd.getTimeInMillis()-cStart.getTimeInMillis()));
			
			fis.close();
			s.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	
}
