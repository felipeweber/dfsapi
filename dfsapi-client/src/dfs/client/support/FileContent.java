package dfs.client.support;

/**
 * Object to hold an array of bytes representing a file content,
 * 	to be sent as parameter to other functions so that the byte array
 * 	doesn't get copied multiple times in the memory
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com> 
 *
 */
public class FileContent {

	private byte[] content;

	public FileContent(byte[] content) {
		this.content = content;
	}
	public byte[] getContent() {
		return content;
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
	
	
	
}
