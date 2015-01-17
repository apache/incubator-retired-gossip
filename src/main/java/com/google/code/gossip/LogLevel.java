package com.google.code.gossip;

public class LogLevel {

	public static final String CONFIG_ERROR = "ERROR";
	
	public static final String CONFIG_INFO = "INFO";
	
	public static final String CONFIG_DEBUG = "DEBUG";
	
	public static final int ERROR = 1;
	public static final int INFO = 2;
	public static final int DEBUG = 3;
	
	public static int fromString(String logLevel) {
		if (logLevel.equals(CONFIG_ERROR))
			return ERROR;
		else if (logLevel.equals(CONFIG_INFO))
			return INFO;
		else if (logLevel.equals(CONFIG_DEBUG))
			return DEBUG;
		else
			return INFO;
	}
}
