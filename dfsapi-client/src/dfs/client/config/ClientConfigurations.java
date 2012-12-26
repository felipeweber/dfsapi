package dfs.client.config;

import dfs.client.caching.replacement.CacheReplacePolicy;
import dfs.client.caching.replacement.CacheReplacePolicyLRU;

public class ClientConfigurations {

	public static int CLIENT_ID = 1;
	public static int FILE_OP_RES_LISTEN_PORT = 60000;
	public static String CLIENT_IP = "172.23.4.2";
	
	public static int MAX_READ_BLOCK_SIZE = 10*1024;
	
	public static Class<? extends CacheReplacePolicy> CACHE_REPLACE_POLICY_CLASS = CacheReplacePolicyLRU.class;
	public static int CACHE_BLOCK_SIZE = 100*1024; // 100K
	public static long MAX_CACHE_SIZE = 1000*1024; // 1MB
	public static boolean FIXED_CACHE_BLOCK_SIZE = false;
	
}
