package whu.edu.cn.trajectory.core.operator.transform.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import whu.edu.cn.trajectory.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.TrajFeatures;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.core.common.constant.TrajectoryDefaultConstant;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class GeoJson2Traj {
  /**
   * Turning GeoJson data into a memory Trajectory object
   *
   * @param value json object
   * @return Trajectory trajectory
   */
  public static Trajectory parseJsonToTrajectory(String value, TrajectoryConfig trajectoryConfig) {
    JSONObject feature = JSONObject.parseObject(value);
    try {
      JSONObject properties = feature.getJSONObject("properties");
      JSONObject geometry = feature.getJSONObject("geometry");
      JSONArray coordinates = geometry.getJSONArray("coordinates");
      String oid = TrajectoryDefaultConstant.oid;
      String tid = TrajectoryDefaultConstant.tid;
      if (!properties.isEmpty()) {
        oid =
            properties.getString(trajectoryConfig.getObjectId().getSourceName()) == null
                ? oid
                : properties.getString(trajectoryConfig.getObjectId().getSourceName());
        tid =
            properties.getString(trajectoryConfig.getTrajId().getSourceName()) == null
                ? tid
                : properties.getString(trajectoryConfig.getTrajId().getSourceName());
      }
      List<TrajPoint> traPoints = parseTraPointList(coordinates, properties, trajectoryConfig);
      if (properties.containsKey("trajectoryFeatures")) {
        JSONObject trajectoryFeatures = (JSONObject) properties.get("trajectoryFeatures");
        TrajFeatures trajFeatures = parseTraFeatures(trajectoryFeatures, properties);
        if (properties.containsKey("extendedValues")) {
          JSONObject extendedValues = (JSONObject) properties.get("extendedValues");
          HashMap<String, Object> extendedValue = new HashMap<>(extendedValues);
          return new Trajectory(tid, oid, traPoints, trajFeatures, extendedValue);
        }
        return new Trajectory(tid, oid, traPoints, trajFeatures);
      }
      return new Trajectory(tid, oid, traPoints);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Trajectory> parseGeoJsonToTrajectoryList(
      String text, TrajectoryConfig trajectoryConfig) {
    JSONObject feature = JSONObject.parseObject(text);
    JSONArray jsonObject = feature.getJSONArray("features");
    ArrayList<Trajectory> trajectories = new ArrayList<>();
    for (int i = 0; i < jsonObject.size(); i++) {
      JSONObject object = jsonObject.getJSONObject(i);
      Trajectory trajectory = parseJsonToTrajectory(object.toString(), trajectoryConfig);
      trajectories.add(trajectory);
    }
    return trajectories;
  }

  public static TrajFeatures parseTraFeatures(JSONObject featureProperties, JSONObject properties) {
    JSONArray mbr = featureProperties.getJSONArray("mbr");
    MinimumBoundingBox box = parseMBR(mbr);

    String sTime = featureProperties.getString("startTime");
    ZonedDateTime startTime = ZonedDateTime.parse(sTime);
    String eTime = featureProperties.getString("endTime");
    ZonedDateTime endTime = ZonedDateTime.parse(eTime);

    JSONArray startPoint = featureProperties.getJSONArray("startPoint");
    TrajPoint traStartPoint = parsePoint(startPoint, properties, true);
    JSONArray endPoint = featureProperties.getJSONArray("endPoint");
    TrajPoint traEndPoint = parsePoint(endPoint, properties, false);

    Integer pointNumber = featureProperties.getInteger("pointNum");
    Double traSpeed = featureProperties.getDouble("speed");

    Double length = featureProperties.getDouble("len");
    return new TrajFeatures(
        startTime, endTime, traStartPoint, traEndPoint, pointNumber, box, traSpeed, length);
  }

  public static TrajPoint parsePoint(JSONArray point, JSONObject properties, Boolean isSTPoint) {
    JSONArray timestamp = properties.getJSONArray("timestamp");
    int pid;
    Long sTime;
    if (isSTPoint) {
      sTime = timestamp.getLong(0);
      pid = 0;
    } else {
      sTime = timestamp.getLong(timestamp.size() - 1);
      pid = timestamp.size() - 1;
    }

    return new TrajPoint(
        Integer.toString(pid),
        BasicDateUtils.timeToZonedTime(sTime),
        point.getDouble(0),
        point.getDouble(1));
  }

  public static MinimumBoundingBox parseMBR(JSONArray mbr) {
    Double lng1 = mbr.getJSONArray(0).getDouble(0);
    Double lat1 = mbr.getJSONArray(0).getDouble(1);
    Double lng2 = mbr.getJSONArray(1).getDouble(0);
    Double lat2 = mbr.getJSONArray(1).getDouble(1);
    return new MinimumBoundingBox(lng1, lat1, lng2, lat2);
  }

  public static Double parseTraSpeed(JSONArray speed) {
    Double traSpeed = 0.0;
    for (int i = 0; i < speed.size(); i++) {
      traSpeed += speed.getDouble(i);
    }
    traSpeed = traSpeed / speed.size();
    return traSpeed;
  }

  public static List<TrajPoint> parseTraPointList(
      JSONArray coordinates, JSONObject properties, TrajectoryConfig trajectoryConfig) {
    ArrayList<TrajPoint> traPointsList = new ArrayList<>();
    JSONArray timestamp = properties.isEmpty() ? null : properties.getJSONArray(trajectoryConfig.getTimeList().getSourceName());
    if (timestamp != null) {
      for (int i = 0; i < coordinates.size(); i++) {
        Long sTime = timestamp.getLong(i);
        TrajPoint trajPoint =
            new TrajPoint(
                Integer.toString(i),
                BasicDateUtils.timeToZonedTime(sTime),
                coordinates.getJSONArray(i).getDouble(0),
                coordinates.getJSONArray(i).getDouble(1));
        traPointsList.add(trajPoint);
      }
    } else {
      for (int i = 0; i < coordinates.size(); i++) {
        TrajPoint trajPoint =
            new TrajPoint(
                Integer.toString(i),
                TrajectoryDefaultConstant.DEFAULT_DATETIME,
                coordinates.getJSONArray(i).getDouble(0),
                coordinates.getJSONArray(i).getDouble(1));
        traPointsList.add(trajPoint);
      }
    }
    return traPointsList;
  }
}
