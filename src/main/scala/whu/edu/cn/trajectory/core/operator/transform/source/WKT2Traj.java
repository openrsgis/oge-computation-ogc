package whu.edu.cn.trajectory.core.operator.transform.source;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.common.constant.TrajectoryDefaultConstant;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class WKT2Traj {
    public static List<Trajectory> parseWKTToTrajectoryList(String value) {
        WKTReader wktReader = new WKTReader();
        ArrayList<Trajectory> trajectories = new ArrayList<>();
        try {
            String[] split = value.split("\n");
            for (String str : split) {
                Geometry geometry = wktReader.read(str);
                String oid = TrajectoryDefaultConstant.oid;
                String tid = TrajectoryDefaultConstant.tid;
                ArrayList<TrajPoint> traPoints = new ArrayList<>();
                Coordinate[] coordinates = geometry.getCoordinates();
                for (int i = 0; i < coordinates.length; i++) {
                    TrajPoint trajPoint =
                            new TrajPoint(
                                    Integer.toString(i),
                                    TrajectoryDefaultConstant.DEFAULT_DATETIME,
                                    coordinates[i].x,
                                    coordinates[i].y);
                    traPoints.add(trajPoint);
                }
                trajectories.add(new Trajectory(tid, oid, traPoints));
            }
            return trajectories;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Trajectory parseWKTToTrajectory(String value) {
        WKTReader wktReader = new WKTReader();
        try {
            Geometry geometry = wktReader.read(value);
            String oid = TrajectoryDefaultConstant.oid;
            String tid = TrajectoryDefaultConstant.tid;
            ArrayList<TrajPoint> traPoints = new ArrayList<>();
            Coordinate[] coordinates = geometry.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                TrajPoint trajPoint =
                        new TrajPoint(
                                Integer.toString(i),
                                TrajectoryDefaultConstant.DEFAULT_DATETIME,
                                coordinates[i].x,
                                coordinates[i].y);
                traPoints.add(trajPoint);
            }
            return new Trajectory(tid, oid, traPoints);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
