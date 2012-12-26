package dfs.datanode;

/**
 * Stores the configuration for this datanode
 * Having a START_PORT allows us to have two datanodes running on the same machine
 * listening on different ports
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DatanodeConfigurations {
	
	public static String ID;
	public static String IP;
	public static int START_PORT;
	public static String ROOT_DIR;
	
	/** MAXIMUM amount of data that will be sent at once */
	public static int NETWORK_MAX_BLOCK_SIZE = 64 * 1024;
	
}
