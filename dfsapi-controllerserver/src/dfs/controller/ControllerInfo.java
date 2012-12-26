package dfs.controller;

/**
 * Stores the configuration for this datanode
 * Having a START_PORT allows us to have two datanodes running on the same machine
 * listening on different ports
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ControllerInfo {
	
	public static String IP;
	public static int START_PORT;
	
}
