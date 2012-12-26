package dfs.common.exceptions;

public class DatanodeInfoException extends Exception {

	private static final long serialVersionUID = 1L;

	private String cause;
	
	public DatanodeInfoException(String cause) {
		this.cause = cause;
	}
	
	public String getMessage() {
		return cause;
	}
	

}
