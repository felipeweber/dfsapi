package dfs.datanode.comm.replication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Holds and manages the threads to communicate between datanodes
 *  and synchronize files by the Replication Pipeline.
 * The threads regarding both sending and receiving blocks of data is managed
 *  in this same thread pool.
 * The number of concurrent threads is determined by the number of processors
 * 	available in the machine where the datanode is running.
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ThreadPoolManager {
	
	private ExecutorService executorService;
	
	private static final ThreadPoolManager INSTANCE =  new ThreadPoolManager();
	private ThreadPoolManager() {
		executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	public static ThreadPoolManager getInstance() {
		return INSTANCE;
	}
	
	public void addThread(Runnable threadToAdd) {
		synchronized (executorService) {
			executorService.submit(threadToAdd);
		}
	}
	
}
