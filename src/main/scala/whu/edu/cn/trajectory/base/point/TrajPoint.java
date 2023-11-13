package whu.edu.cn.trajectory.base.point;

import whu.edu.cn.trajectory.base.util.BasicDateUtils;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/6
 */

public class TrajPoint extends BasePoint implements Serializable {
    private String pid;
    private String timeString;
    private ZonedDateTime timestamp;
    private Map<String, Object> extendedValues;

    public TrajPoint(ZonedDateTime timestamp, double lng, double lat) {
        super(lng, lat);
        this.pid = null;
        this.timestamp = timestamp;
        this.extendedValues = null;
    }

    public TrajPoint(String timeString, double lng, double lat) {
        super(lng, lat);
        this.timeString = timeString;
    }

    public TrajPoint(String id, ZonedDateTime timestamp, double lng, double lat) {
        super(lng, lat);
        this.pid = id;
        this.timestamp = timestamp;
        this.extendedValues = null;
    }

    public TrajPoint(String id, String timeString, double lng, double lat) {
        super(lng, lat);
        this.pid = id;
        this.timeString = timeString;
    }

    public TrajPoint(String id, ZonedDateTime timestamp, double lng, double lat,
                     Map<String, Object> extendedValues) {
        super(lng, lat);
        this.pid = id;
        this.timestamp = timestamp;
        this.extendedValues = extendedValues;
    }

    public TrajPoint(String id, String timeString, double lng, double lat,
                     Map<String, Object> extendedValues) {
        super(lng, lat);
        this.pid = id;
        this.timeString = timeString;
        this.extendedValues = extendedValues;
    }

    public void setTimeString(String timeString) {
        this.timeString = timeString;
    }

    public void setTimestamp(ZonedDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getPid() {
        return pid;
    }

    public ZonedDateTime getTimestamp() {
        if (timestamp == null) {
            timestamp = BasicDateUtils.parseDate(timeString);
        }
        return timestamp;
    }

    public String getTimestampString() {
        return timeString;
    }

    public Map<String, Object> getExtendedValues() {
        return this.extendedValues == null ? null : this.extendedValues;
    }

    public void setExtendedValues(Map<String, Object> extendedValues) {
        this.extendedValues = extendedValues;
    }

    public Object getExtendedValue(String key) {
        return this.extendedValues == null ? null : this.extendedValues.get(key);
    }

    public void setExtendedValue(String key, Object value) {
        if (this.extendedValues == null) {
            this.extendedValues = new HashMap<>();
        }
        this.extendedValues.put(key, value);
    }

    public void removeExtendedValue(String key) {
        if (this.extendedValues != null) {
            this.extendedValues.remove(key);
        }
    }


    public String toString() {
        return "TrajPoint{pid='" + this.pid + '\'' + ", longitude=" + this.getLng() + ", latitude="
                + this.getLat() + ", timeStamp=" + this.getTimestamp() + ", extendedValues="
                + this.extendedValues + '}';
    }
}
