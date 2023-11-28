package whu.edu.cn.trajectory.core.operator.load;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.data.IDataConfig;
import whu.edu.cn.trajectory.core.conf.data.TrajPointConfig;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;
import whu.edu.cn.trajectory.core.conf.load.StandaloneLoadConfig;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.conf.load.HDFSLoadConfig;
import whu.edu.cn.trajectory.core.conf.load.ILoadConfig;
import whu.edu.cn.trajectory.core.operator.transform.ParquetTransform;
import whu.edu.cn.trajectory.core.operator.transform.source.*;

import javax.ws.rs.NotSupportedException;
import java.io.File;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HDFSLoader implements ILoader {
  private static final Logger LOGGER = Logger.getLogger(HDFSLoader.class);

  public JavaRDD<Trajectory> loadTrajectory(
      SparkSession sparkSession, ILoadConfig loadConfig, IDataConfig dataConfig) {
    if (loadConfig instanceof HDFSLoadConfig && dataConfig instanceof TrajectoryConfig) {
      HDFSLoadConfig hdfsLoadConfig = (HDFSLoadConfig) loadConfig;
      TrajectoryConfig trajectoryConfig = (TrajectoryConfig) dataConfig;
      switch (hdfsLoadConfig.getFileMode()) {
        case MULTI_FILE:
          return this.loadTrajectoryFromMultiFile(sparkSession, hdfsLoadConfig, trajectoryConfig);
        case SINGLE_FILE:
          return this.loadTrajectoryFromSingleFile(sparkSession, hdfsLoadConfig, trajectoryConfig);
        case MULTI_SINGLE_FILE:
          return this.loadTrajectoryFromMultiSingleFile(
              sparkSession, hdfsLoadConfig, trajectoryConfig);
        default:
          throw new NotSupportedException(
              "can't support fileMode " + hdfsLoadConfig.getFileMode().getMode());
      }
    } else {
      LOGGER.error(
          "This loadConfig is not a HDFSLoadConfig or this dataConfig is not a "
              + "TrajectoryConfig！Please check your config file");
      throw new RuntimeException(
          "loadConfig is not a HDFSLoadConfig or dataConfig is not a TrajectoryConfig in "
              + "configuration json file");
    }
  }

  @Override
  public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig) {
    throw new NotImplementedError();
  }

  private JavaRDD<Trajectory> loadTrajectoryFromParquet(
      SparkSession sparkSession, HDFSLoadConfig hdfsLoadConfig, TrajectoryConfig trajectoryConfig, boolean isTraj) {
    String filterText = hdfsLoadConfig.getFilterText();
    String location = hdfsLoadConfig.getLocation();
    File directory = new File(location);
    File[] files = directory.listFiles();
    JavaRDD<Trajectory> unionJavaRDD = null;
    if (files != null) {
      for (File file : files) {
        String fileName = file.getName();
        if (file.isFile() && fileName.endsWith(".parquet")) {
          JavaRDD<Trajectory> trajectoryJavaRDD = null;
          if (isTraj) {
            trajectoryJavaRDD =
                ParquetTransform.loadParquetTrajData(
                    file.getAbsolutePath(), sparkSession, filterText, hdfsLoadConfig.getPartNum());
          } else {
            trajectoryJavaRDD =
                ParquetTransform.loadParquetPointData(
                    file.getAbsolutePath(),
                    sparkSession,
                    filterText,
                    trajectoryConfig,
                    hdfsLoadConfig.getPartNum());
          }
          if (unionJavaRDD == null) {
            unionJavaRDD = trajectoryJavaRDD;
          } else {
            unionJavaRDD.union(trajectoryJavaRDD);
          }
        }
      }
    }
    return unionJavaRDD;
  }

  private JavaRDD<Trajectory> loadTrajectoryFromMultiFile(
      SparkSession sparkSession, HDFSLoadConfig hdfsLoadConfig, TrajectoryConfig trajectoryConfig) {
    LOGGER.info("Loading trajectories from multi_files in folder: " + hdfsLoadConfig.getLocation());
    int partNum = hdfsLoadConfig.getPartNum();
    FileTypeEnum fileType = hdfsLoadConfig.getFileType();
    if (fileType == FileTypeEnum.parquet) {
      return loadTrajectoryFromParquet(sparkSession, hdfsLoadConfig, trajectoryConfig, false);
    }
    JavaRDD<Tuple2<String, String>> loadRDD =
        sparkSession
            .sparkContext()
            .wholeTextFiles(hdfsLoadConfig.getLocation(), partNum)
            .toJavaRDD()
            .filter(
                (s) -> {
                  return !(s._2).isEmpty();
                });
    JavaRDD<Trajectory> resultRdd = null;
    switch (fileType) {
      case csv:
        resultRdd =
            loadRDD
                .map(
                    (s) -> {
                      Trajectory trajectory =
                          CSV2Traj.multifileParse(
                              s._2(), trajectoryConfig, hdfsLoadConfig.getSplitter());
                      if (trajectory != null && trajectoryConfig.getTrajId().getIndex() < 0) {
                        String[] strings = (s._1).split("/");
                        String name = strings[strings.length - 1];
                        String trajId = name.substring(0, name.lastIndexOf("."));
                        trajectory.setTrajectoryID(trajId);
                      }
                      return trajectory;
                    })
                .filter(Objects::nonNull);
        break;
      case wkt:
        resultRdd =
            loadRDD.map((s) -> WKT2Traj.parseWKTToTrajectory(s._2())).filter(Objects::nonNull);
        break;
      case geojson:
        resultRdd =
            loadRDD
                .map((s) -> GeoJson2Traj.parseJsonToTrajectory(s._2(), trajectoryConfig))
                .filter(Objects::nonNull);
        break;
      case shp:
        resultRdd =
            loadRDD
                .map((s) -> Shp2Traj.parseShapefileToTrajectory(s._1, trajectoryConfig))
                .filter(Objects::nonNull);
        break;
      case kml:
        resultRdd =
            loadRDD
                .map((s) -> KML2Traj.parseKMLToTrajectory(s._1, trajectoryConfig))
                .filter(Objects::nonNull);
        break;
      default:
        throw new NotSupportedException(
            "can't support fileType " + hdfsLoadConfig.getFileType().getFileTypeEnum());
    }
    return resultRdd;
  }

  private JavaRDD<Trajectory> loadTrajectoryFromSingleFile(
      SparkSession sparkSession, HDFSLoadConfig hdfsLoadConfig, TrajectoryConfig trajectoryConfig) {
    LOGGER.info("Loading trajectories from single file : " + hdfsLoadConfig.getLocation());
    int partNum = hdfsLoadConfig.getPartNum();
    FileTypeEnum fileType = hdfsLoadConfig.getFileType();
    if (fileType == FileTypeEnum.parquet) {
      return loadTrajectoryFromParquet(sparkSession, hdfsLoadConfig, trajectoryConfig, true);
    }
    JavaRDD<Tuple2<String, String>> loadRDD =
        sparkSession
            .sparkContext()
            .wholeTextFiles(hdfsLoadConfig.getLocation(), partNum)
            .toJavaRDD()
            .filter(
                (s) -> {
                  return !(s._2).isEmpty();
                });
    JavaRDD<Trajectory> resultRdd = null;
    switch (fileType) {
      case csv:
        loadRDD
            .flatMap(
                s -> {
                  return CSV2Traj.singlefileParse(
                          s._2, trajectoryConfig, hdfsLoadConfig.getSplitter())
                      .iterator();
                })
            .filter(Objects::nonNull);
        break;
      case wkt:
        resultRdd =
            loadRDD
                .flatMap(s -> WKT2Traj.parseWKTToTrajectoryList(s._2).iterator())
                .filter(Objects::nonNull);
        break;
      case geojson:
        resultRdd =
            loadRDD
                .flatMap(
                    (s ->
                        GeoJson2Traj.parseGeoJsonToTrajectoryList(s._2, trajectoryConfig)
                            .iterator()))
                .filter(Objects::nonNull);
        break;
      case shp:
        resultRdd =
            loadRDD
                .flatMap(
                    s -> Shp2Traj.parseShapefileToTrajectoryList(s._1, trajectoryConfig).iterator())
                .filter(Objects::nonNull);
        break;
      case kml:
        resultRdd =
            loadRDD
                .flatMap(s -> KML2Traj.parseKMLToTrajectoryList(s._1, trajectoryConfig).iterator())
                .filter(Objects::nonNull);
        break;
      default:
        throw new NotSupportedException(
            "can't support fileType " + hdfsLoadConfig.getFileType().getFileTypeEnum());
    }
    return resultRdd;
  }

  private JavaRDD<Trajectory> loadTrajectoryFromMultiSingleFile(
      SparkSession sparkSession, HDFSLoadConfig hdfsLoadConfig, TrajectoryConfig trajectoryConfig) {
    LOGGER.info("Loading trajectories from multi_files in folder: " + hdfsLoadConfig.getLocation());
    int partNum = hdfsLoadConfig.getPartNum();
    return sparkSession
        .sparkContext()
        .wholeTextFiles(hdfsLoadConfig.getLocation(), partNum)
        .toJavaRDD()
        .filter((s) -> !(s._2).isEmpty())
        .flatMap(
            (s) ->
                CSV2Traj.singlefileParse(s._2, trajectoryConfig, hdfsLoadConfig.getSplitter())
                    .iterator())
        .filter(Objects::nonNull);
  }
}
