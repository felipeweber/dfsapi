package dfs.client.comm.fileoperations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FileOperationsReqThreadPool {

	private ExecutorService executorService;
	
	private static final FileOperationsReqThreadPool INSTANCE =  new FileOperationsReqThreadPool();
	
	private FileOperationsReqThreadPool() {
		executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	
	public static FileOperationsReqThreadPool getInstance() {
		return INSTANCE;
	}
	
	@SuppressWarnings("unchecked")
	public Future<FileOperationsReadReqThread> addThread(FileOperationsReadReqThread threadToAdd) {
		synchronized (executorService) {
			Future<FileOperationsReadReqThread> future = (Future<FileOperationsReadReqThread>) executorService.submit(threadToAdd);
			return future;
		}
	}
	
}
