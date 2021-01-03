package com.course.kafka.util;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class LocalDateTimeUtil22 {

	public static long toEpochTimestamp22(LocalDateTime localDateTime) {
		return localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}
}

/*
this class we have created just for converting LocalDateTime into Epoch milliseconds
 */