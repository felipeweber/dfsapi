package dfs.controller.filestructure;

import java.util.Date;

public class DFSLock {

	private String acquiredBy;
	private Date acquiredOn;
	
	public String getAcquiredBy() {
		return acquiredBy;
	}
	public void setAcquiredBy(String acquiredBy) {
		this.acquiredBy = acquiredBy;
	}
	public Date getAcquiredOn() {
		return acquiredOn;
	}
	public void setAcquiredOn(Date acquiredOn) {
		this.acquiredOn = acquiredOn;
	}
	
	
	
}