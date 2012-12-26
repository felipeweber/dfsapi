package dfs.controller.comm.state;

import dfs.common.protocol.objects.FileOperationProtos.ClientIdentifierProto;

public class Operation {

	private Long id;
	
	private ClientIdentifierProto clientIdentifier;
	/**
	 * operationType refers to a valid OperationType value
	 */
	private int operationType;
	private Object paramProto;
	
	
	public Operation(Long id, ClientIdentifierProto clientIdentifier, int operationType, Object paramProto) {
		this.id = id;
		this.clientIdentifier = clientIdentifier;
		this.operationType = operationType;
		this.paramProto = paramProto;
	}
	
	@Override
	public String toString() {
		StringBuilder ret = new StringBuilder();
		ret.append("[id: " + id + "; clientIdentifier: " + clientIdentifier + "; operationType: " + operationType + "; paramProto: " + paramProto);
		return ret.toString();
	}

	public Long getId() {
		return id;
	}
	public ClientIdentifierProto getClientIdentifier() {
		return clientIdentifier;
	}
	public int getOperationType() {
		return operationType;
	}
	public Object getParamProto() {
		return paramProto;
	}
	
}
