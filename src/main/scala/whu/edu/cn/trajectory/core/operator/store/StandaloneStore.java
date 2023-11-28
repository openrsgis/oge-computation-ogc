package whu.edu.cn.trajectory.core.operator.store;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import whu.edu.cn.trajectory.base.point.StayPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.store.StandaloneStoreConfig;
import whu.edu.cn.trajectory.core.conf.store.StoreTypeEnum;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.operator.transform.ParquetTransform;
import whu.edu.cn.trajectory.core.operator.transform.sink.*;
import whu.edu.cn.trajectory.core.operator.transform.source.GeoJson2Traj;
import whu.edu.cn.trajectory.core.operator.transform.source.KML2Traj;
import whu.edu.cn.trajectory.core.operator.transform.source.Shp2Traj;
import whu.edu.cn.trajectory.core.operator.transform.source.WKT2Traj;
import whu.edu.cn.trajectory.core.util.IOUtils;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StandaloneStore implements IStore {

  private StandaloneStoreConfig storeConfig;

  StandaloneStore(StandaloneStoreConfig storeConfig) {
    this.storeConfig = storeConfig;
  }

  public void storePointBasedTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws IOException {
    FileTypeEnum fileType = storeConfig.getFileType();
    String fileStoreName = null;
    List<Trajectory> trajectoryList = null;
    if (fileType != FileTypeEnum.csv) {
      trajectoryList = trajectoryJavaRDD.collect();
      fileStoreName =
          trajectoryList.get(0).getTrajectoryID() == null
              ? String.format(
                  "%s/%s%s",
                  storeConfig.getLocation(),
                  trajectoryList.get(0).getObjectID(),
                  storeConfig.getFilePostFix())
              : String.format(
                  "%s/%s-%s%s",
                  storeConfig.getLocation(),
                  trajectoryList.get(0).getObjectID(),
                  trajectoryList.get(0).getTrajectoryID(),
                  storeConfig.getFilePostFix());
    }

    switch (fileType) {
      case csv:
        trajectoryJavaRDD
            .filter(Objects::nonNull)
            .foreach(
                item -> {
                  String fileName;
                  if (item.getObjectID() == null) {
                    fileName =
                        String.format(
                            "%s/%s%s",
                            storeConfig.getLocation(),
                            item.getTrajectoryID(),
                            storeConfig.getFilePostFix());
                  } else {
                    fileName =
                        String.format(
                            "%s/%s-%s%s",
                            storeConfig.getLocation(),
                            item.getObjectID(),
                            item.getTrajectoryID(),
                            storeConfig.getFilePostFix());
                  }
                  String outputString = Traj2CSV.convert(item, storeConfig.getSplitter());
                  IOUtils.writeStringToFile(fileName, outputString);
                });
        break;
      case wkt:
        String wktStr = Traj2WKT.convertTrajListToWKT(trajectoryList);
        IOUtils.writeStringToFile(fileStoreName, wktStr);
        break;
      case geojson:
        JSONObject jsonObject = Traj2GeoJson.convertTrajListToGeoJson(trajectoryList);
        IOUtils.writeStringToFile(fileStoreName, jsonObject.toString());
        break;
      case shp:
        Traj2Shp.createShapefile(fileStoreName, trajectoryList);
        break;
      case kml:
        Traj2KML.convertTrajListToKML(fileStoreName, trajectoryList);
        break;
      default:
        throw new NotSupportedException(
            "can't support fileType " + storeConfig.getFileType().getFileTypeEnum());
    }
  }

  @Override
  public void storeTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD) throws IOException {
    switch (this.storeConfig.getSchema()) {
      case POINT_BASED_TRAJECTORY:
        this.storePointBasedTrajectory(trajectoryJavaRDD);
        return;
      default:
        throw new NotImplementedError();
    }
  }

  @Override
  public void storeTrajectory(JavaRDD<Trajectory> trajectoryJavaRDD, SparkSession ss) throws Exception {
    switch (this.storeConfig.getSchema()) {
      case POINT_BASED_TRAJECTORY:
        this.storePointBasedTrajectoryUseSpark(trajectoryJavaRDD, ss);
        return;
      default:
        throw new NotImplementedError();
    }
  }
  public void storePointBasedTrajectoryUseSpark(JavaRDD<Trajectory> trajectoryJavaRDD, SparkSession ss) throws IOException {
    FileTypeEnum fileType = storeConfig.getFileType();
    switch (fileType){
      case parquet:
        ParquetTransform.storeTrajAsParquet(trajectoryJavaRDD, ss, storeConfig.getLocation());
        break;
      default:
        throw new NotSupportedException(
                "can't support fileType " + storeConfig.getFileType().getFileTypeEnum());
    }
  }

  @Override
  // TODO 单stayPoint 解析
  public void storeStayPointList(JavaRDD<List<StayPoint>> stayPointJavaRDD) {
    stayPointJavaRDD.foreach(
        s -> {
          if (!s.isEmpty()) {
            String outputString = StayPoint2CSV.convertSPList(s, storeConfig.getSplitter());
            String fileName =
                String.format(
                    "%s/%s-splist%s",
                    storeConfig.getLocation(),
                    s.get(0).getSid().split("_")[0],
                    storeConfig.getFilePostFix());
            IOUtils.writeStringToFile(fileName, outputString);
          }
        });
  }

  @Override
  public void storeStayPointASTraj(JavaRDD<StayPoint> stayPointJavaRDD) {
    stayPointJavaRDD.foreach(
        s -> {
          String outputString = StayPoint2CSV.convertSPAsTraj(s, storeConfig.getSplitter());
          String fileName =
              String.format(
                  "%s/%s-splist%s",
                  storeConfig.getLocation(), s.getSid(), storeConfig.getFilePostFix());
          IOUtils.writeStringToFile(fileName, outputString);
        });
  }
}
