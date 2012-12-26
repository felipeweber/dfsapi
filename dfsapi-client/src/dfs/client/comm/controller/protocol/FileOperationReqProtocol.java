package dfs.client.comm.controller.protocol;

import dfs.common.protocol.objects.FileOperationProtos.FileInfoReqProto;
import dfs.common.protocol.objects.FileOperationProtos.FilePathProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileReadControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.FileWriteControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.MkDirControllerResponseProto;
import dfs.common.protocol.objects.FileOperationProtos.OperationIdentifierProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchClientRequestProto;
import dfs.common.protocol.objects.FileOperationProtos.TouchControllerResponseProto;


/**
 * Methods that implement the protocol to communicate client and controller
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public interface FileOperationReqProtocol {

	public MkDirControllerResponseProto mkDir(MkDirClientRequestProto mkDirRequestProto) throws Exception ;
	
	public TouchControllerResponseProto touch(TouchClientRequestProto touchRequestProto) throws Exception ;

	public FileWriteControllerResponseProto write(FileWriteClientRequestProto writerProto) throws Exception;

	public FileReadControllerResponseProto read(FileReadClientRequestProto readProto)  throws Exception;
	
	public int retrieveFileInfo(FileInfoReqProto fileInfoReqProto) throws Exception;;
	
	@Deprecated
	public OperationIdentifierProto ls(FilePathProto path) throws Exception;

	
}
