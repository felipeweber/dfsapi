package dfs.common.protocol.configurations;

/**
 * Constants used to define an operation type in the whole protocol
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class OperationType {
	
	public static final int END_CONN = -1;
	public static final int FILE_OP_LS = 1;
	public static final int FILE_OP_MKDIR = 2;
	public static final int FILE_OP_TOUCH = 3;
	public static final int FILE_OP_WRITE = 4;
	public static final int FILE_OP_READ = 5;
	public static final int FILE_OP_REQ_NEXT_DATANODE = 6;
	public static final int FILE_OP_INFO = 7;

}
