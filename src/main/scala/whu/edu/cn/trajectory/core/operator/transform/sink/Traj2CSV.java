package whu.edu.cn.trajectory.core.operator.transform.sink;

import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class Traj2CSV implements Serializable {
    public static String convert(Trajectory trajectory,
                                 String splitter,
                                 String timePattern) {
        if (trajectory.getPointList() == null) {
            return null;
        }
        String tid = trajectory.getTrajectoryID();
        String oid = trajectory.getObjectID();
        StringBuilder records = new StringBuilder();
        List<TrajPoint> pointList = trajectory.getPointList();
        // 属性字段非空，则按照原始属性输出
        if (pointList.get(0).getExtendedValues() != null) {
            for (TrajPoint tmpP : pointList) {
                StringBuilder record = new StringBuilder();
                for (Map.Entry<String, Object> stringObjectEntry : tmpP.getExtendedValues().entrySet()) {
                    record.append(stringObjectEntry.getValue()).append(splitter);
                }
                record.deleteCharAt(record.lastIndexOf(splitter));
                records.append(record).append(System.lineSeparator());
            }
        } else {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timePattern);
            for (TrajPoint tmpP : pointList) {
                StringBuilder record = new StringBuilder();
                record.append(tid).append(splitter);
                record.append(oid).append(splitter);
                record.append(tmpP.getPid()).append(splitter);
                record.append(tmpP.getLat()).append(splitter);
                record.append(tmpP.getLng()).append(splitter);
                record.append(tmpP.getTimestamp().format(formatter));
                if (null != tmpP.getExtendedValues()) {
                    for (Map.Entry<String, Object> set : tmpP.getExtendedValues().entrySet()) {
                        record.append(splitter).append(set.getValue());
                    }
                }
                records.append(record).append(System.lineSeparator());
            }
        }

        return records.toString();
    }

    public static String convert(Trajectory trajectory,
                                 String splitter) {
        return convert(trajectory, splitter, "yyyy-MM-dd HH:mm:ss");
    }
}
