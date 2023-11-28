package whu.edu.cn.trajectory.base.trajectory;

import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.point.TrajPointWithoutGeometry;
import whu.edu.cn.trajectory.base.util.SerializerUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/28
 */
public class TrajectoryWithoutGeometry implements Serializable {
  private String trajectoryID;
  private String objectID;
  private byte[] pointList;

  public TrajectoryWithoutGeometry(String trajectoryID, String objectID, List<TrajPoint> pointList)
      throws IOException {
    this.trajectoryID = trajectoryID;
    this.objectID = objectID;
    this.pointList = SerializerUtils.serializeList(
            parseTrajPointToWithoutGeometry(pointList), TrajPointWithoutGeometry.class);
  }

  public String getTrajectoryID() {
    return trajectoryID;
  }

  public void setTrajectoryID(String trajectoryID) {
    this.trajectoryID = trajectoryID;
  }

  public String getObjectID() {
    return objectID;
  }

  public void setObjectID(String objectID) {
    this.objectID = objectID;
  }

  public byte[] getPointList() {
    return pointList;
  }

  public void setPointList(List<TrajPoint> pointList) throws IOException {
    this.pointList = SerializerUtils.serializeList(
            parseTrajPointToWithoutGeometry(pointList), TrajPointWithoutGeometry.class);
  }

  public static List<TrajPointWithoutGeometry> parseTrajPointToWithoutGeometry(
      List<TrajPoint> pointList) {
    List<TrajPointWithoutGeometry> trajPointWithoutGeometries = new ArrayList<>();
    for (TrajPoint trajPoint : pointList) {
      TrajPointWithoutGeometry trajPointWithoutGeometry =
          new TrajPointWithoutGeometry(
              trajPoint.getPid(), trajPoint.getTimestampString(), trajPoint.getCoordinate());
      trajPointWithoutGeometries.add(trajPointWithoutGeometry);
    }
    return trajPointWithoutGeometries;
  }

  public static List<TrajPoint> parseWithoutGeometryToTrajPoint(
      List<TrajPointWithoutGeometry> trajPointWithoutGeometries) {
    ArrayList<TrajPoint> trajPoints = new ArrayList<>();
    for (TrajPointWithoutGeometry trajPointWithoutGeometry : trajPointWithoutGeometries) {
      TrajPoint trajPoint = new TrajPoint(
              trajPointWithoutGeometry.getPid(),
              trajPointWithoutGeometry.getTimeString(),
              trajPointWithoutGeometry.getCoordinate().x,
              trajPointWithoutGeometry.getCoordinate().y);
      trajPoints.add(trajPoint);
    }
    return trajPoints;
  }
}
