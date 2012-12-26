package dfs.common.configparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import dfs.common.exceptions.DatanodeInfoException;

/**
 * Read datanodes configuration file (default: config/common/datanodesaddress.conf)
 * and tries to parse the datanodes configurations for name, ip and startPort
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class DatanodeInfoParser {

	//public static String configurationFile = "/home/felipe/dfsapi/dfsapi/dfsapi-common/config/common/common.conf";
	//public static String configurationFile = "E:\\Meus Documentos\\My Dropbox\\ULBRA\\TCC\\dfsapi\\dfsapi\\dfsapi-common\\config\\common\\common.conf";
	//public static String configurationFile = "common.conf"; 
	//public static String configurationFile = "C:\\dfsadpi\\common.conf";
	public static String configurationFile = ConfigurationFile.configurationFile;
	
	/**
	 * Get the datanode metadata (ip, startport, rootdir, ...) by its id. 
	 * Used on the datanode initialization
	 * 
	 * @param id
	 * @return String[4]: ID IP START_PORT ROOT_DIR (String String int String)
	 * @throws DatanodeInfoException
	 */
	public static String[] parseDatanodeInfo(String id) throws DatanodeInfoException {
		File file = new File(configurationFile);
		FileInputStream f;
		
		try {
			f = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new DatanodeInfoException("ERROR: configuration file not found [" + configurationFile + "]");
		}
		
		BufferedReader in = new BufferedReader(new InputStreamReader(f));
		
		String line;
		try {
			while(true) {
				line = in.readLine();
				if (line == null)
					break;
				
				String[] tokens = line.split(" ");
				
				// found the line with the config for this datanode
				//  parse one by one to be sure all is how it should be
				if (tokens[0].equals(id)) {
					try {
						String[] ret = new String[4];
						ret[0] = id;
						ret[1] = tokens[1];
						// verify that the port is numeric
						Integer.parseInt(tokens[2]);
						ret[2] = tokens[2];
						ret[3] = tokens[3];
						return ret;
					} catch (Exception e) {
						throw new DatanodeInfoException("ERROR: malformed configuration line for " + id + " : " + line);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DatanodeInfoException("ERROR: error reading from file " + configurationFile);
		}
		
		try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		throw new DatanodeInfoException("ERROR: datanode name " + id + " not found on configuration file");
		
	}
	
	
	/**
	 * Get the datanode metadata (ip, startport, rootdir, ...) for all datanodes. 
	 * Used on client-datanode communication
	 * 
	 * @param id
	 * @return List<String[4]>: ID IP START_PORT ROOT_DIR (String String int String)
	 * @throws DatanodeInfoException
	 */
	public static List<String[]> parseDatanodesInfo() throws DatanodeInfoException {
		File file = new File(configurationFile);
		FileInputStream f;
		
		try {
			f = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new DatanodeInfoException("ERROR: configuration file not found [" + configurationFile + "]");
		}
		
		BufferedReader in = new BufferedReader(new InputStreamReader(f));

		List<String[]> ret = new ArrayList<String[]>();
		
		
		String line;
		try {
			while(true) {
				line = in.readLine();
				if (line == null)
					break;
				
				if (line.trim().equals(""))
					continue;
				
				if (line.startsWith("#"))
					continue;
				
				if (line.startsWith("controllerServer"))
					continue;
				
				String[] tokens = line.split(" ");
				
				// found the line with the config for a datanode
				//  parse one by one to be sure all is how it should be
				try {
					String[] lineTmp = new String[4];
					lineTmp[0] = tokens[0];
					lineTmp[1] = tokens[1];
					// verify that the port is numeric
					Integer.parseInt(tokens[2]);
					lineTmp[2] = tokens[2];
					lineTmp[3] = tokens[3];
					ret.add(lineTmp);
				} catch (Exception e) {
					System.out.println("WARNING: malformed configuration line " + line);
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DatanodeInfoException("ERROR: error reading from file " + configurationFile);
		}
		
		return ret;
		
	}

	
}
