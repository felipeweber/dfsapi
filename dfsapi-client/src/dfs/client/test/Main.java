package dfs.client.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import dfs.client.api.DFSClient;
import dfs.client.api.DFSFileManager;
import dfs.client.caching.CacheManager;
import dfs.client.caching.replacement.CacheReplacePolicyLFU;
import dfs.client.caching.replacement.CacheReplacePolicyLRU;
import dfs.client.config.ClientConfigurations;
import dfs.common.logger.Logger;

public class Main {

	public static void main(String args[]) {
		try {
			DFSClient client = new DFSClient();
			ClientConfigurations.CLIENT_ID = 1;
			ClientConfigurations.FILE_OP_RES_LISTEN_PORT = 60000;
			if (client.startListeners()) {
				// Define Cache attributes and Replacement Policy
				ClientConfigurations.CACHE_BLOCK_SIZE = 50*1024; // 
				ClientConfigurations.MAX_CACHE_SIZE = 500*1024*1024; // 
				ClientConfigurations.CACHE_REPLACE_POLICY_CLASS = CacheReplacePolicyLFU.class;
				
				// write 1 50MB file
				// writeFile("50MB", "50MB1");
				
				// read 1 50MB file
				readFile("50MB1");
				
			} else {
				System.out.println("ERROR: Error starting client");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.exit(0);
		 
	}
	private static void writeFile(String subFolder, String fileName) throws Exception {
		Timer.start();
		long timeOverhead = Timer.end();
		File f;
		String folder = "/home/felipe/Dropbox/ULBRA/TCC/dfsapi/arquivos_teste/"+subFolder+"/";
		Timer.start();
		f = new File(folder+fileName);
		FileInputStream fis = new FileInputStream(f);
		byte[] bytes = new byte[(int)f.length()];
		while(fis.read(bytes)!=-1);
		DFSFileManager.write("/felipe/", fileName, bytes);
		fis.close();
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
	}
	
	private static void readFile(String file) throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		//Logger.logStats("DFSAPIRequest: Lendo um arquivo de 50MB", Main.class);
		Timer.start();
		byte[] bytes = DFSFileManager.read("felipe", file, 0, 0);
		//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
	}
	
}
