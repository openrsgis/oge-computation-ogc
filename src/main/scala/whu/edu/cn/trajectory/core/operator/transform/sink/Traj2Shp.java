package whu.edu.cn.trajectory.core.operator.transform.sink;

import org.geotools.data.DefaultTransaction;
import org.geotools.data.Transaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.trajectory.TrajLine;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.common.constant.GeoConstant;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class Traj2Shp {
  public static final String GEOM_FIELD_NAME = "the_geom";
  private static final Logger logger = LoggerFactory.getLogger(Traj2Shp.class);

  public static void createShapefile(String outputPath, List<Trajectory> trajectoryList)
      throws IOException {
    File file = new File(outputPath);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
    // 创建Shapefile的FeatureCollection
    DefaultFeatureCollection featureCollection = getFeatureCollection(trajectoryList);
    createDataStore(file, featureCollection);
  }
  public static void createShapefile(String outputPath, Trajectory trajectory)
          throws IOException {
    File file = new File(outputPath);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      file.createNewFile();
    }
    // 创建Shapefile的FeatureCollection
    DefaultFeatureCollection featureCollection = getFeatureCollection(trajectory);
    createDataStore(file, featureCollection);
  }

  public static void createDataStore(File file, DefaultFeatureCollection featureCollection)
      throws IOException {
    // 创建Shapefile的DataStore
    Map<String, Serializable> params = new HashMap<>();
    params.put(ShapefileDataStoreFactory.URLP.key, file.toURI().toURL());
    params.put("create spatial index", Boolean.TRUE);
    ShapefileDataStoreFactory dataStoreFactory = new ShapefileDataStoreFactory();
    ShapefileDataStore dataStore = (ShapefileDataStore) dataStoreFactory.createNewDataStore(params);
    SimpleFeatureType featureType = getFeatureType(GeoConstant.crs);
    dataStore.createSchema(featureType);
    // 创建Transaction
    Transaction transaction = new DefaultTransaction("create");
    try {
      String typeName = dataStore.getTypeNames()[0];
      SimpleFeatureSource featureSource = dataStore.getFeatureSource(typeName);
      // 将FeatureCollection写入Shapefile
      SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
      featureStore.setTransaction(transaction);
      try {
        featureStore.addFeatures(featureCollection);
        logger.info("Shapefile transform start");
        transaction.commit();
      } catch (IOException problem) {
        transaction.rollback();
      } finally {
        logger.info("Shapefile has been store in " + file.toURI().toURL());
        transaction.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static DefaultFeatureCollection getFeatureCollection(List<Trajectory> trajectoryList) {
    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
    SimpleFeature simpleFeature = null;
    for (Trajectory trajectory : trajectoryList) {
      simpleFeature = bulidFeature(trajectory);
      featureCollection.add(simpleFeature);
    }
    return featureCollection;
  }

  public static DefaultFeatureCollection getFeatureCollection(Trajectory trajectory) {
    DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();
    SimpleFeature simpleFeature = bulidFeature(trajectory);
    featureCollection.add(simpleFeature);
    return featureCollection;
  }

  public static SimpleFeature bulidFeature(Trajectory trajectory) {
    SimpleFeatureType featureType = getFeatureType(GeoConstant.crs);

    // 构建Feature并添加到FeatureCollection中
    SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
    featureBuilder.add(trajectory.getLineStringAsDate());
    featureBuilder.add(trajectory.getObjectID());
    featureBuilder.add(trajectory.getTrajectoryID());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getStartTime());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getEndTime());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getStartPoint().getPointSequence());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getEndPoint().getPointSequence());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getPointNum());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getSpeed());
    featureBuilder.add(trajectory.getTrajectoryFeatures().getLen());

    return featureBuilder.buildFeature(null);
  }

  public static SimpleFeatureType getFeatureType(CoordinateReferenceSystem crs) {
    // 构建Shapefile的属性字段
    SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    builder.setName("LineStringType");
    builder.setCRS(crs);
    builder.add(GEOM_FIELD_NAME, TrajLine.class);
    builder.add("oid", String.class);
    builder.add("tid", String.class);
    builder.add("startTime", ZonedDateTime.class);
    builder.add("endTime", ZonedDateTime.class);
    builder.add("startPoint", String.class);
    builder.add("endPoint", String.class);
    builder.add("pointNum", Integer.class);
    builder.add("speed", Double.class);
    builder.add("len", Double.class);

    return builder.buildFeatureType();
  }
  private static AttributeType createStringType(int maxLength) {
    AttributeTypeBuilder typeBuilder = new AttributeTypeBuilder();
    typeBuilder.binding(String.class);
    typeBuilder.length(maxLength);
    return typeBuilder.buildType();
  }
}
