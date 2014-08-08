package com.craftedbytes.utils;

import org.apache.log4j.PropertyConfigurator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class LoggerStarter {

	//private static final String LOGPATH = "C:\\work\\log4j.properties";
    private static final String LOGPATH = "/home/ubuntu/AdtooxHZDeploymentLinux/work/log4j.properties";
	
	public  static void initLogger() {
		try {
			Properties p = new Properties();
			p.load(new FileInputStream(LOGPATH));
			PropertyConfigurator.configure(p);
		} catch (IOException e) {
			System.err.println("Logger failed to initialzie..." + e.getStackTrace());
		}
	}
}