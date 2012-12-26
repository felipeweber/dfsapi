package dfs.common.logger;

public class Logger {

	public static boolean isDebug = false;
	public static boolean isStatistics = false;
	public static boolean isInfo = true;
	
	public static void logDebug(String message, Class<?> origClass) {
		if (isDebug) {
			System.out.println(origClass.getPackage().getName() + origClass.getName() + " :: " + message);
		}
	}
	public static void logInfo(String message) {
		if (isInfo) {
			System.out.println("INFO: " + message);
		}
	}
	public static void logInfo(String message, Class<?> origClass) {
		if (isInfo) {
			System.out.println("INFO: " + origClass.getPackage().getName() + origClass.getName() + " :: " + message);
		}
	}
	public static void logStats(String message, Class<?> origClass) {
		if (isStatistics) {
			System.out.println("Statistics :: " + message);
		}
	}
	
	public static void logError(String message, Class<?> origClass) {
		System.err.println("ERROR: " + origClass.getPackage().getName() + origClass.getName() + " :: " + message);
	}
	
	public static void logError(String message, Throwable e, Class<?> origClass) {
		System.err.println("ERROR: " + origClass.getPackage().getName() + origClass.getName() + " :: " + message);
		e.printStackTrace();
	}
	
}
