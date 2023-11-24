package whu.edu.cn.trajectory.core.operator.transform.sink;

import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class Traj2WKT {
  private static final Logger logger = LoggerFactory.getLogger(Traj2WKT.class);

  public static String convertTrajListToWKT(List<Trajectory> trajectoryList) {
    StringBuilder stringBuilder = new StringBuilder();
    for (Trajectory trajectory : trajectoryList) {
      LineString lineString = trajectory.getLineString();
      WKTWriter wktWriter = new WKTWriter();
      String writeWKT = wktWriter.write(lineString);
      stringBuilder.append(writeWKT).append("\n");
    }
    return stringBuilder.toString();
  }

  public static String convertTrajToWKT(Trajectory trajectory) {
    StringBuilder stringBuilder = new StringBuilder();
    LineString lineString = trajectory.getLineString();
    WKTWriter wktWriter = new WKTWriter();
    String writeWKT = wktWriter.write(lineString);
    stringBuilder.append(writeWKT).append("\n");
    return stringBuilder.toString();
  }
}
