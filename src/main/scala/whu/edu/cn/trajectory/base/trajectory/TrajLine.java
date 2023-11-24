package whu.edu.cn.trajectory.base.trajectory;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/23
 */
public class TrajLine extends LineString {

    private List<TrajPoint> trajPointList;
    private Object userData = null;
    public TrajLine(CoordinateSequence points, GeometryFactory factory) {
        super(points, factory);
    }
    public TrajLine(CoordinateSequence points, GeometryFactory factory, List<TrajPoint> pointList) {
        super(points, factory);
        this.trajPointList = pointList;
    }

    public List<TrajPoint> getTrajPointList() {
        return trajPointList;
    }

    public void setTrajPointList(List<TrajPoint> trajPointList) {
        this.trajPointList = trajPointList;
    }

    @Override
    public Object getUserData() {
        return userData;
    }

    @Override
    public void setUserData(Object userData) {
        this.userData = userData;
    }

    public Coordinate[] getCoordinates() {
        Coordinate[] coordinates = new Coordinate[trajPointList.size()];
        ArrayList<Long> timestamp = new ArrayList<>();
        for (int i = 0; i < trajPointList.size(); i++) {
            TrajPoint trajPoint = trajPointList.get(i);
            coordinates[i] = new Coordinate(trajPoint.getX(), trajPoint.getY());
            long timeStamp = BasicDateUtils.parseDateToTimeStamp(
                    trajPoint.getTimestamp());
            timestamp.add(timeStamp);
        }
        userData = timestamp;
        return coordinates;
    }
}
