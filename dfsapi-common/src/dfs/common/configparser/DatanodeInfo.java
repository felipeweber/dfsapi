package dfs.common.configparser;

public class DatanodeInfo {

	private String id;
	private String host;
	private int startPort;
	
	public DatanodeInfo(String id, String host, int startPort) {
		this.id = id;
		this.host = host;
		this.startPort = startPort;
	}
	
	@Override
	public String toString() {
		return "[Id: " + id + " ; host: " + host + " ; startPort: " + startPort + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof String) {
			return ((String)obj).equals(this.id);
		} else if (obj instanceof DatanodeInfo) {
			return ((DatanodeInfo)obj).getId().equals(this.id);
		}
		return super.equals(obj);
	}

	public String getId() {
		return id;
	}
	public String getHost() {
		return host;
	}
	public int getStartPort() {
		return startPort;
	}

	public boolean isValid() {
		return host != null && !host.isEmpty()
				&& startPort != 0;
	}
	
}
