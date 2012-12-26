package dfs.client.comm.state;

/**
 * Semaphore to indicate when a datanode responds through the listeners
 * and also contains the response itself
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DatanodeOperationSemaphore {

	private int operationType;
	// something we might have to send to the datanode (such as a file content)
	private Object parameter;
	// a response the datanode might send
	private Object response;
	
	public DatanodeOperationSemaphore(int operationType) {
		this.operationType = operationType;
	}
	
	public DatanodeOperationSemaphore(int operationType, Object parameter) {
		super();
		this.operationType = operationType;
		this.parameter = parameter;
	}

	
	public int getOperationType() {
		return operationType;
	}
	public void setOperationType(int operationType) {
		this.operationType = operationType;
	}
	public Object getResponse() {
		return response;
	}
	public void setResponse(Object response) {
		this.response = response;
	}
	public Object getParameter() {
		return parameter;
	}
	public void setParameter(Object parameter) {
		this.parameter = parameter;
	}
	
}
