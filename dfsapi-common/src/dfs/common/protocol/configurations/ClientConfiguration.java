package dfs.common.protocol.configurations;


/**
 * General configurations regarding the clients
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ClientConfiguration {

	/**
	 * Time (in seconds) that the informations regarding a datanode
	 * connection are still valid. After that, they should be revalidated
	 */
	public static int datanodesUpdateTimeout = 300; 
	
}
