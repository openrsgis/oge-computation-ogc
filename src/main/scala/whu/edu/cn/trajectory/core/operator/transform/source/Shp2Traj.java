package whu.edu.cn.trajectory.core.operator.transform.source;

import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.core.common.constant.TrajectoryDefaultConstant;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @author xuqi
 * @date 2023/11/20
 */
public class Shp2Traj {
  public static Trajectory parseShapefileToTrajectory(
      String sourcePath, TrajectoryConfig trajectoryConfig) throws IOException {
    File file = new File(sourcePath);
    ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
    Map<String, Serializable> params = new HashMap<>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
    params.put("create spatial index", Boolean.TRUE);
    DataStore dataStore = dataStoreFactory.createNewDataStore(params);
    String typeName = dataStore.getTypeNames()[0];
    SimpleFeatureIterator iterator = dataStore.getFeatureSource(typeName).getFeatures().features();
    SimpleFeature feature = iterator.next();
    return parseFeatureToTrajectory(feature, trajectoryConfig);
  }

  public static List<Trajectory> parseShapefileToTrajectoryList(
      String sourcePath, TrajectoryConfig trajectoryConfig) throws IOException {
    File file = new File(sourcePath);
    ArrayList<Trajectory> trajectories = new ArrayList<>();
    ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
    Map<String, Serializable> params = new HashMap<>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.getPath());
    params.put("create spatial index", Boolean.TRUE);
    DataStore dataStore = dataStoreFactory.createNewDataStore(params);
    String typeName = dataStore.getTypeNames()[0];
    SimpleFeatureIterator iterator = dataStore.getFeatureSource(typeName).getFeatures().features();
    while (iterator.hasNext()){
      SimpleFeature feature = iterator.next();
      Trajectory trajectory = parseFeatureToTrajectory(feature, trajectoryConfig);
      trajectories.add(trajectory);
    }
    return trajectories;
  }

  public static Trajectory parseFeatureToTrajectory(
      SimpleFeature feature, TrajectoryConfig trajectoryConfig) {
    String oid = (String) feature.getAttribute(trajectoryConfig.getObjectId().getSourceName());
    String tid = (String) feature.getAttribute(trajectoryConfig.getTrajId().getSourceName());
    List<TrajPoint> trajPoints = parseFeatureGeoToPointsList(feature);
    return new Trajectory(tid, oid, trajPoints);
  }

  public static List<TrajPoint> parseFeatureGeoToPointsList(SimpleFeature feature) {
    Geometry geometry = (Geometry) feature.getDefaultGeometry();
    List<TrajPoint> trajPoints = new ArrayList<>();
    Coordinate[] coordinates = geometry.getCoordinates();
    if(coordinates[0].getZ() > 0){
      for (Coordinate coordinate : coordinates) {
        TrajPoint trajPoint =
                new TrajPoint(
                        BasicDateUtils.timeToZonedTime((long) coordinate.z), coordinate.x, coordinate.y);
        trajPoints.add(trajPoint);
      }
    }else {
      for (Coordinate coordinate : coordinates) {
        TrajPoint trajPoint =
                new TrajPoint(
                        TrajectoryDefaultConstant.DEFAULT_DATETIME, coordinate.x, coordinate.y);
        trajPoints.add(trajPoint);
      }
    }

    return trajPoints;
  }
}
