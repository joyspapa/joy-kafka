package com.joy.kafka.monitor.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtils {

	public static String getNormalDate() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(new Date());
    }
	
	public static String getDateHour() {
        SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
        return df.format(new Date());
    }
}
