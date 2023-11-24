package whu.edu.cn.trajectory.base.point;

import whu.edu.cn.trajectory.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajectory.base.util.CheckUtils;
import whu.edu.cn.trajectory.base.util.GeoUtils;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StayPoint implements Serializable {
    private String sid;
    private String oid;
    private ZonedDateTime startTime;
    private ZonedDateTime endTime;
    private MinimumBoundingBox mbr;

    private List<TrajPoint> plist;
    private BasePoint centerPoint;

    public StayPoint(List plist, String sid, String oid) {
        if (CheckUtils.isCollectionEmpty(plist)) {
            throw new RuntimeException("ptList is expected not empty.");
        } else {
            this.plist = plist;
            this.sid = sid;
            this.oid = oid;
            this.startTime = ((TrajPoint) plist.get(0)).getTimestamp();
            this.endTime = ((TrajPoint) plist.get(plist.size() - 1)).getTimestamp();
            this.mbr = GeoUtils.calMinimumBoundingBox(plist);
            double lngSum = 0.0;
            double latSum = 0.0;

            TrajPoint tmpP;
            for (Iterator iter = plist.iterator(); iter.hasNext(); latSum += tmpP.getLat()) {
                tmpP = (TrajPoint) iter.next();
                lngSum += tmpP.getLng();
            }
//      Point centroid = new Trajectory("", "", plist).getLineString().convexHull().getCentroid();
//      this.centerPoint = new BasePoint(centroid.getX(), centroid.getY());
            this.centerPoint =
                    new BasePoint(lngSum / (double) plist.size(), latSum / (double) plist.size());
        }
    }

    public long getStayTimeInSecond() {
        return ChronoUnit.SECONDS.between(this.startTime, this.endTime);
    }

    public BasePoint getCenterPoint() {
        return centerPoint;
    }

    public String getSid() {
        return sid;
    }

    public String getOid() {
        return oid;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public ZonedDateTime getEndTime() {
        return endTime;
    }

    public MinimumBoundingBox getMbr() {
        return mbr;
    }

    public void setMbr(MinimumBoundingBox mbr) {
        this.mbr = mbr;
    }

    public List<TrajPoint> getPlist() {
        return plist;
    }

    @Override
    public String toString() {
        return "StayPoint{"
                + "sid='" + sid + '\''
                + ", oid='" + oid + '\''
                + ", startTime=" + startTime
                + ", endTime=" + endTime
                + ", mbr=" + mbr
                + ", plist=" + plist
                + ", centerPoint=" + centerPoint
                + '}';
    }
}
