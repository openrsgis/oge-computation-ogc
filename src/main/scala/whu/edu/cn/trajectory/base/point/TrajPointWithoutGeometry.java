package whu.edu.cn.trajectory.base.point;

import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/28
 */
public class TrajPointWithoutGeometry implements Serializable {
    private String pid;
    private String timeString;
    private Coordinate coordinate;

    public TrajPointWithoutGeometry(String pid, String timeString, Coordinate coordinate) {
        this.pid = pid;
        this.timeString = timeString;
        this.coordinate = coordinate;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getTimeString() {
        return timeString;
    }

    public void setTimeString(String timeString) {
        this.timeString = timeString;
    }

    public Coordinate getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(Coordinate coordinate) {
        this.coordinate = coordinate;
    }

    @Override
    public String toString() {
        return "TrajPointWithoutGeometry{" +
                "pid='" + pid + '\'' +
                ", timeString='" + timeString + '\'' +
                ", coordinate=" + coordinate +
                '}';
    }
}
