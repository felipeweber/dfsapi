package dfs.client.comm.fileoperations;

import dfs.common.logger.Logger;
import dfs.common.protocol.objects.FileOperationProtos.FileReadClientRequestProto;

public class FileOperationsReadReqThread extends Thread {

	String path;
	String filename;
	int offset;
	int size;
	
	byte[] contentResponse;
	
	public FileOperationsReadReqThread(String path, String filename, int offset, int size) {
		this.path = path;
		this.filename = filename;
		this.offset = offset;
		this.size = size;
	}
	
	@Override
	public void run() {
		try {
			FileReadClientRequestProto.Builder readProto = FileReadClientRequestProto.newBuilder();
			readProto.setFilename(filename);
			readProto.setPath(path);
			readProto.setOffset(offset);
			readProto.setSize(size);
			
			contentResponse = FileOperationsComm.read(readProto.build());
		} catch (Exception e) {
			Logger.logError("CRITICAL ERROR: COULD NOT READ FROM DFS: " + path + "/" + filename, e, getClass());
		}
	}

	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public byte[] getContentResponse() {
		return contentResponse;
	}
	public void setContentResponse(byte[] contentResponse) {
		this.contentResponse = contentResponse;
	}
	

}
