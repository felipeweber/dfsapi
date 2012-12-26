package dfs.common.protocol.configurations;


/**
 * Holds the valid values for the InternalConnectionType attribute
 * Those attributes are used both to identify an operation type but, 
 * specially, to determine which port a server is listening for a given operation
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class InternalConnectionType {

	public static String STATISTICS_GATHERING = "SG";
	public static String FILE_OPERATIONS_INTERN_TYPE = "FO";
	public static String CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE = "FR";
	public static String DATANODES_REPLICATION_PIPELINE_OPERATIONS = "RP";
	
	// valid connection types
	private static final String[] CONNECTION_TYPES = 
												{
													STATISTICS_GATHERING, 		// statistics gathering
													FILE_OPERATIONS_INTERN_TYPE,  // general file operations
													CONTROLLER_FILE_OPERATIONS_RES_INTERN_TYPE, // file operation response
													DATANODES_REPLICATION_PIPELINE_OPERATIONS // operations performed between datanodes for the Replication Pipeline
												};
	
	// respective sum_ports for each connection type
	private static final int[] SUM_PORTS = 
									{
										0,	// statistics gathering
										1,	// general file operations
										2, 	// Controller only file operation response
										2	// Datanodes only Replication Pipeline operations
									};
	
	public static int getSumPort(String internalConnectionType) throws Exception {
		for (int i=0; i<CONNECTION_TYPES.length;i++) {
			if (CONNECTION_TYPES[i].equals(internalConnectionType)) {
				return SUM_PORTS[i];
			}
		}
		throw new Exception("ERROR: INVALID INTERNAL_CONNECTION_TYPE");
	}
	
	public static String findConnectionTypeByOperation(int operationType)  throws Exception {
		switch(operationType) {
		// file operations
		case OperationType.FILE_OP_LS:
		case OperationType.FILE_OP_WRITE:
		case OperationType.FILE_OP_READ:
		case OperationType.FILE_OP_INFO:
			return FILE_OPERATIONS_INTERN_TYPE;
		}
		
		throw new Exception("ERROR: INVALID OPERATION_TYPE");
	}
	
}