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

public class MainBKP {

	public static void main(String args[]) {
		try {
			DFSClient client = new DFSClient();
			ClientConfigurations.CLIENT_ID = 1;
			ClientConfigurations.FILE_OP_RES_LISTEN_PORT = 60000;
			if (client.startListeners()) {
				// Define Cache attributes and Replacement Policy
				ClientConfigurations.CACHE_BLOCK_SIZE = 5*1024; // 
				ClientConfigurations.MAX_CACHE_SIZE = 500*1024*1024; // 
				ClientConfigurations.CACHE_REPLACE_POLICY_CLASS = CacheReplacePolicyLFU.class;
				
				// write 1 50MB file
				// writeFile("50MB", "50MB0");
				
				// read 1 50MB file
				readFile("50MB0");
				
				
				
				// read 1 50MB file
				
				/** TESTING READ **/
				// read50k25FirstTwiceThenRest();
				
				// readOne5MBFileTwice();
				
				// readOne50MBFileTwice();
				
				// stressTestBigFragmentedCache();
				
				// stressTestBigNotFragmentedCache();
				
				
				// stressTestReadFromCacheSeparateTimes();
				
				/*for(int i=0;i<1;i++) {
				//read50k25FirstTwiceThenRest();
				// get time overhead for calling the function
				Timer.start();
				long timeOverhead = Timer.end();
				System.out.println("TIMER: time overhead is " + timeOverhead);
				
				Timer.start();
				Logger.logStats("DFSAPIRequest: Lendo todos de uma vez", Main.class);
				for(int j=0; j<100; j++) {
					byte[] bytes = DFSFileManager.read("felipe", "50k"+j, 0, 0);
					File f = new File("/home/felipe/desenv/workspaces/dfsapi/client/50k"+j);
					FileOutputStream fos = new FileOutputStream(f);
					fos.write(bytes);
					fos.close();
					bytes = null;
				}
				System.out.println("TIMER: " + (Timer.end()-timeOverhead));
				}*/
				
				
				/** WRITES **/
				// write 100 50k files to the system
				// writeFiles(100, "50k");
				
				// write 50 5MB files to the system
				// writeFiles(50, "5MB");
				
				// write 6 50MB files to the system
				// writeFiles(6, "50MB");
				
				//testWrite();
				
				//testRead();
				
			} else {
				System.out.println("ERROR: Error starting client");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.exit(0);
		 
	}
	
	private static void testWrite() {
		//File f = new File("C:\\dfsadpi\\teste.txt");
		//File f = new File("E:\\spring_workspace\\teste.txt");
		File f = new File("/home/felipe/desenv/workspaces/dfsapi/teste.txt");
		try {
			FileInputStream fis = new FileInputStream(f);
			byte[] bytes = new byte[(int)f.length()];
			try {
				while(fis.read(bytes)!=-1);
				
				try {
					DFSFileManager.write("/felipe/", "teste.txt", bytes);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	
	private static void testRead() throws InterruptedException {
		
		long start = new Date().getTime();
		long timeOverhead = new Date().getTime() - start;
		/*try {
			byte[] bytes = DFSFileManager.read("felipe", "teste.txt", 1024000, 50000);
			//File f2 = new File("/home/felipe/teste_read.txt");
			//File f2 = new File("c:\\dfsadpi\\teste_read.txt");
			//FileOutputStream out = new FileOutputStream(f2);
			//out.write(bytes);
			//out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Chamada 1: " + (new Date().getTime() - start));
		Thread.sleep(1000);*/
		start = new Date().getTime();
		try {
			byte[] bytes = DFSFileManager.read("felipe", "teste.txt", 1000000, 2000000);
			//File f2 = new File("/home/felipe/teste_read.txt");
			//File f2 = new File("c:\\dfsadpi\\teste_read.txt");
			//FileOutputStream out = new FileOutputStream(f2);
			//out.write(bytes);
			//out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Chamada 2: " + (new Date().getTime() - start));
		Thread.sleep(1000);
		start = new Date().getTime();
		try {
			byte[] bytes = DFSFileManager.read("felipe", "teste.txt", 0, 0);
			File f2 = new File("/home/felipe/teste_read.txt");
			//File readOutFile = new File("c:\\dfsadpi\\teste_read.txt");
			FileOutputStream readOutStream = new FileOutputStream(f2);
			readOutStream.write(bytes);
			readOutStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Chamada 3: " + (new Date().getTime() - start));
		
	}
	
	private static void testLs() {
		List<String> fileList = DFSFileManager.ls("/");
		for (String string : fileList) {
			System.out.println(string);
		}
		
		fileList = DFSFileManager.ls("/");
		for (String string : fileList) {
			System.out.println(string);
		}
	}
	

	private static void writeFiles(int numberOfFiles, String fileNamePattern) throws Exception {
		Timer.start();
		long timeOverhead = Timer.end();
		File f;
		String folder = "/home/felipe/Dropbox/ULBRA/TCC/dfsapi/arquivos_teste/"+fileNamePattern+"/";
		Timer.start();
		for(int i=0; i<numberOfFiles; i++) {
			f = new File(folder+fileNamePattern+i);
			FileInputStream fis = new FileInputStream(f);
			byte[] bytes = new byte[(int)f.length()];
			while(fis.read(bytes)!=-1);
			DFSFileManager.write("/felipe/", fileNamePattern+i, bytes);
			fis.close();
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
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
	
	private static void read50k25FirstTwiceThenRest() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo todos de uma vez", MainBKP.class);
		for(int i=0; i<100; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo todos de uma vez", MainBKP.class);
		for(int i=0; i<100; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo primeiros 25 vez 1", MainBKP.class);
		for(int i=0; i<25; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo primeiros 25 vez 2", MainBKP.class);
		for(int i=0; i<25; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo segundo 25 vez 1", MainBKP.class);
		for(int i=25; i<50; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Timer.start();
		Logger.logStats("DFSAPIRequest: Lendo primeiros 25 vez 3", MainBKP.class);
		for(int i=0; i<25; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
	}
	
	
	private static void readOne5MBFileTwice() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		//Logger.logStats("DFSAPIRequest: Lendo um arquivo de 5MB", Main.class);
		Timer.start();
		byte[] bytes = DFSFileManager.read("felipe", "5MB1", 0, 0);
		//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		//Logger.logStats("DFSAPIRequest: Lendo mesmo arquivo de 5MB", Main.class);
		for(int i=0; i<20; i++) {
			Timer.start();
			bytes = DFSFileManager.read("felipe", "5MB1", 0, 0);
			//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
			System.out.println((Timer.end()-timeOverhead));
		}
		
	}
	
	
	private static void readOne50MBFileTwice() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		//Logger.logStats("DFSAPIRequest: Lendo um arquivo de 50MB", Main.class);
		Timer.start();
		byte[] bytes = DFSFileManager.read("felipe", "50MB0", 0, 0);
		//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		//Logger.logStats("DFSAPIRequest: Lendo mesmo arquivo de 50MB", Main.class);
		for(int i=0;i<20;i++) {
			Timer.start();
			bytes = DFSFileManager.read("felipe", "50MB0", 0, 0);
			//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
			System.out.println((Timer.end()-timeOverhead));
		}
		
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
	
	private static void stressTestBigFragmentedCache() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		Logger.logStats("DFSAPIRequest: Lendo 100 arquivos de 50K", MainBKP.class);
		Timer.start();
		for(int i=0; i<100; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Logger.logStats("DFSAPIRequest: Lendo 50 arquivos de 5MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<50; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "5MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Logger.logStats("DFSAPIRequest: Lendo 1 arquivo de 50MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<1; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		System.out.println("Blocks on cache: " + CacheManager.getNumberOfBlocksOnCache());
		
		//Logger.logStats("DFSAPIRequest: Lendo 1 arquivo de 5MB", Main.class);
		for(int i=0; i<20; i++) {
			Timer.start();
			byte[] bytes = DFSFileManager.read("felipe", "5MB1", 0, 0);
			System.out.println((Timer.end()-timeOverhead));
		}
		//System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		
		//Logger.logStats("DFSAPIRequest: Lendo 1 arquivo de 50MB", Main.class);
		for(int i=0; i<20; i++) {
			Timer.start();
			byte[] bytes = DFSFileManager.read("felipe", "50MB0", 0, 0);
			System.out.println((Timer.end()-timeOverhead));
		}
		//System.out.println("TIMER: " + (Timer.end()-timeOverhead));*/
	}
	
	private static void stressTestBigNotFragmentedCache() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		Logger.logStats("DFSAPIRequest: Lendo 6 arquivos de 50MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<6; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Logger.logStats("DFSAPIRequest: Lendo 6 arquivos de 50MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<1; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
	}
	
	private static void stressTestReadFromCacheSeparateTimes() throws Exception {
		// get time overhead for calling the function
		Timer.start();
		long timeOverhead = Timer.end();
		System.out.println("TIMER: time overhead is " + timeOverhead);
		
		Logger.logStats("DFSAPIRequest: Lendo 100 arquivos de 50K", MainBKP.class);
		Timer.start();
		for(int i=0; i<100; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Logger.logStats("DFSAPIRequest: Lendo 50 arquivos de 5MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<50; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "5MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		Logger.logStats("DFSAPIRequest: Lendo 1 arquivo de 50MB", MainBKP.class);
		Timer.start();
		for(int i=0; i<1; i++) {
			byte[] bytes = DFSFileManager.read("felipe", "50MB"+i, 0, 0);
		}
		System.out.println("TIMER: " + (Timer.end()-timeOverhead));
		
		System.out.println("Blocks on cache: " + CacheManager.getNumberOfBlocksOnCache());
		
		//Logger.logStats("DFSAPIRequest: Lendo 1 arquivo de 50MB", Main.class);
		for(int i=0; i<20; i++) {
			Timer.start();
			byte[] bytes = DFSFileManager.read("felipe", "50MB0", 0, 0);
			System.out.println((Timer.end()-timeOverhead));
		}
		//
		for(int j=0;j<3;j++) {
			for(int i=0; i<100; i++) {
				byte[] bytes = DFSFileManager.read("felipe", "50k"+i, 0, 0);
			}
			for(int i=0; i<50; i++) {
				byte[] bytes = DFSFileManager.read("felipe", "5MB"+i, 0, 0);
			}
		}
		
		System.out.println("Lendo 50MB pela segunda vez");
		for(int i=0; i<20; i++) {
			Timer.start();
			byte[] bytes = DFSFileManager.read("felipe", "50MB0", 0, 0);
			System.out.println((Timer.end()-timeOverhead));
		}
		
	}

}
