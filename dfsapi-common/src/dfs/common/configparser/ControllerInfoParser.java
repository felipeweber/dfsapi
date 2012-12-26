package dfs.common.configparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import dfs.common.exceptions.DatanodeInfoException;

/**
 * Read configuration file (default: common.conf)
 * and tries to parse the controller configuration
 * 
 * @author Felipe Weber <wbr.felipe@gmail.com>
 *
 */
public class ControllerInfoParser {

	//public static String configurationFile = "/home/felipe/dfsapi/dfsapi/dfsapi-common/config/common/common.conf"; 
	//public static String configurationFile = "common.conf";
	//public static String configurationFile = "C:\\dfsadpi\\common.conf";
	//public static String configurationFile = "E:\\Meus Documentos\\My Dropbox\\ULBRA\\TCC\\dfsapi\\dfsapi\\dfsapi-common\\config\\common\\common.conf";
	public static String configurationFile = ConfigurationFile.configurationFile;
	
	/**
	 * Get the controller server data (ControllerServer IP START_PORT) 
	 * 
	 * @param id
	 * @throws DatanodeInfoException
	 */
	public static String[] parseControllerServerInfo() throws Exception {
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
				
				if (line.startsWith("#"))
					continue;
				
				String[] tokens = line.split(" ");
				
				// found the line with the config for the controller server
				//  parse one by one to be sure all is how it should be
				if (tokens[0].equalsIgnoreCase("controllerServer")) {
					try {
						String[] ret = new String[2];
						ret[0] = tokens[1];
						// verify that the port is numeric
						Integer.parseInt(tokens[2]);
						ret[1] = tokens[2];
						return ret;
					} catch (Exception e) {
						throw new DatanodeInfoException("ERROR: malformed configuration line for controllerServer: " + line);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DatanodeInfoException("ERROR: error reading from file " + configurationFile);
		}
		
		throw new DatanodeInfoException("ERROR: ControllerServer configuration not found on configuration file");
		
	}
	
}
