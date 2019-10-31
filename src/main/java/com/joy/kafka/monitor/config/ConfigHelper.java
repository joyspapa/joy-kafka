package com.joy.kafka.monitor.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {

	private static final Logger logger = LoggerFactory.getLogger(ConfigHelper.class);

	private static final String propertyFileName = "config-viewer.properties";

	private static String bus_kafka_monitor = "bus_kafka_monitor";
	private static String bus_kafka_admin = "bus_kafka_admin";
	private static String brokers = "192.168.10.82:9092,192.168.10.83:9092,192.168.10.84:9092";
	private static int listenPort = -1;
	private static Properties prop;

	static {
		try {
			loadConfig();
		} catch (Exception ex) {
			logger.error(">>>>>>> Main Error (System.exit(1)) : ", ex);
			System.exit(1);
		}
	}

	private static void loadConfig() throws Exception {
		prop = new Properties();
		try (InputStream is = ConfigHelper.class.getClassLoader().getResourceAsStream(propertyFileName)) {
			prop.load(is);
			
			String readPort = prop.getProperty("synapse.offset.viewer.rest.port");
			if(readPort != null) {
				listenPort = Integer.parseInt(readPort);
			}
			
//			String readBrokers = prop.getProperty("synapse.kafka.monitor.broker");
//			if(readBrokers != null) {
//				brokers = readBrokers;
//			}
//			String readBusMonitor = prop.getProperty("synapse.kafka.monitor.bus");
//			if(readBusMonitor != null) {
//				bus_kafka_monitor = readBusMonitor;
//			}
//			String readBusAdmin = prop.getProperty("synapse.kafka.admin.bus");
//			if(readBusAdmin != null) {
//				bus_kafka_admin = readBusAdmin;
//			}
			logger.info("It has done loading the " + propertyFileName);
		} catch (IOException e) {
			logger.error("Error in initializing rest properties", e);
			throw e;
		}
	}

	public static void reloadConfig() throws Exception {
		loadConfig();
	}
	
	public static int getPort() {
		return listenPort;
	}
	
	public static String getValue(String key) {
		try {
			return getProperties().getProperty(key);
		} catch (Exception ex) {
			return "";
		}
	}
	
	public static String getValue(String key, String defaultValue) {
		try {
			String value = getProperties().getProperty(key);
			if (value == null)
				return defaultValue;
			else
				return value;
		} catch (Exception ex) {
			return "";
		}
	}

	private static Properties getProperties() throws Exception {
		if (prop == null) {
			loadConfig();
		}
		return prop;
	}

	private Set<Object> getAllKeySet() {
		try {
			return getProperties().keySet();
		} catch (Exception ex) {
			logger.error("Error in getKeySet", ex);
			return null;
		}
	}

	private String getKeySetWithPattern(String keyPattern) {
		try {
			Iterator<Object> iter = getProperties().keySet().iterator();
			String key;
			while (iter.hasNext()) {
				key = (String) iter.next();
				if (key.contains(keyPattern)) {
					return key;
				}
			}

			return "";

		} catch (Exception ex) {
			logger.error("Error in getKeySet", ex);
			return "";
		}
	}

	public static void setProperties(String key, String value) {
		try {
			getProperties().setProperty(key, value);
		} catch (Exception ex) {
			logger.error("Error in initializing rest setProperties", ex);
		}
	}
}