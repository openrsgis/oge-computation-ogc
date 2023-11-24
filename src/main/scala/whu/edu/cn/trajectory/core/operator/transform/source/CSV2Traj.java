package whu.edu.cn.trajectory.core.operator.transform.source;

import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class CSV2Traj {
    public static Trajectory multifileParse(String rawString,
                                            TrajectoryConfig trajectoryConfig,
                                            String splitter) throws IOException {
        String[] points = rawString.split(System.lineSeparator());
        int n = points.length;
        List<TrajPoint> trajPoints = new ArrayList<>(n);
        String trajId = "";
        String objectId = "";
        String pStr;
        boolean genPid = false;
        for (int i = 0; i < n; ++i) {
            pStr = points[i];
            if (i == 0) {
                String[] firstP = pStr.split(splitter);
                int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
                int trajIdIndex = trajectoryConfig.getTrajId().getIndex();
                if (trajIdIndex >= 0) {
                    trajId = firstP[trajIdIndex];
                }
                if (objectIdIndex >= 0) {
                    objectId = firstP[objectIdIndex];
                }
            }
            TrajPoint point =
                    CSV2TrajPoint.parse(pStr, trajectoryConfig.getTrajPointConfig(),
                            splitter);
            if (point.getPid() == null) {
                genPid = true;
            }
            trajPoints.add(point);
        }
        return trajPoints.isEmpty() ? null : new Trajectory(trajId, objectId, trajPoints, genPid);
    }

    public static List<Trajectory> singlefileParse(String rawString,
                                                   TrajectoryConfig trajectoryConfig,
                                                   String splitter) throws IOException {
        int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
        int trajIdIndex = trajectoryConfig.getTrajId().getIndex();
        String[] points = rawString.split(System.lineSeparator());
        // 按tid+oid分组
        Map<String, List<String>> groupList = Arrays.stream(points).collect(
                Collectors.groupingBy(item -> getGroupKey(item, splitter, trajIdIndex, objectIdIndex)));
        // 映射
        return groupList.values().stream()
                .map(item -> {
                    try {
                        return mapToTraj(item, splitter, trajIdIndex, objectIdIndex, trajectoryConfig);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static String getGroupKey(String line, String splitter, int trajIdIndex,
                                      int objectIdIndex) {
        String[] tmpP = line.split(splitter);
        return tmpP[trajIdIndex] + "#" + tmpP[objectIdIndex];
    }

    private static Trajectory mapToTraj(List<String> points, String splitter, int trajIdIndex,
                                        int objectIdIndex, TrajectoryConfig trajectoryConfig)
            throws IOException {
        String trajId = "", objectId = "";
        List<TrajPoint> trajPoints = new ArrayList<>(points.size());
        for (String point : points) {
            String[] tmpP = point.split(splitter);
            trajId = tmpP[trajIdIndex];
            objectId = tmpP[objectIdIndex];
            TrajPoint trajPoint =
                    CSV2TrajPoint.parse(point, trajectoryConfig.getTrajPointConfig(),
                            splitter);
            trajPoints.add(trajPoint);
        }
        if (!trajPoints.isEmpty()) {
            trajPoints.sort(
                    (o1, o2) -> {
                        return (int)
                                (o1.getTimestamp().toEpochSecond() - o2.getTimestamp().toEpochSecond());
                    });
            return new Trajectory(trajId, objectId, trajPoints);
        } else {
            return null;
        }
    }
}
