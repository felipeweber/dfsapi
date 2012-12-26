package dfs.client.test;

import java.util.Date;

public class Timer {

	public static long timeStart;
	
	public static void start() {
		timeStart = new Date().getTime();
	}
	
	public static long end() {
		return new Date().getTime() - timeStart;
	}
	
}
