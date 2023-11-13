package whu.edu.cn.trajectory.base.util;

import org.apache.commons.lang.StringUtils;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author xuqi
 * @date 2023/11/06
 */
public class BasicDateUtils {

    static String deaultFormat = "yyyy-MM-dd HH:mm:ss";
    static String defaultZoneId = "UTC+8";

    static DateTimeFormatter defaultFormatter =
            DateTimeFormatter.ofPattern(deaultFormat).withZone(ZoneId.of(defaultZoneId));

    public static void updateStaticProperties(String newFormat, String newZoneId) {
        if (newFormat != null && !newFormat.isEmpty()) {
            deaultFormat = newFormat;
        }
        if (newZoneId != null && !newZoneId.isEmpty()) {
            defaultZoneId = newZoneId;
        }
        defaultFormatter = DateTimeFormatter.ofPattern(deaultFormat).withZone(ZoneId.of(defaultZoneId));
    }

    public static String getDeaultFormat() {
        return deaultFormat;
    }

    public static String getDefaultZoneId() {
        return defaultZoneId;
    }

    public static DateTimeFormatter getDefaultFormatter() {
        return defaultFormatter;
    }

    public static ZonedDateTime timeToUTC(ZonedDateTime time) {
        return time.withZoneSameInstant(ZoneOffset.UTC);
    }

    public static ZonedDateTime parse(String time, DateTimeFormatter dateTimeFormatter) {
        return StringUtils.isEmpty(time.trim()) ? null :
                ZonedDateTime.parse(time.trim(), dateTimeFormatter);
    }

    public static String format(ZonedDateTime time, String pattern) {
        return time == null ? "" : DateTimeFormatter.ofPattern(pattern).format(time);
    }

    public static ZonedDateTime parseDate(String timeFormat) {
        return ZonedDateTime.parse(timeFormat, defaultFormatter);
    }

    public static ZonedDateTime parseDate(String timeFormat, String format, String zoneId) {
        if (StringUtils.isEmpty(format)) {
            format = "yyyy-MM-dd HH:mm:ss";
        }

        if (StringUtils.isEmpty(zoneId)) {
            zoneId = "UTC+8";
        }

        DateTimeFormatter timeFormatter =
                DateTimeFormatter.ofPattern(format).withZone(ZoneId.of(zoneId));
        return ZonedDateTime.parse(timeFormat, timeFormatter);
    }
}
