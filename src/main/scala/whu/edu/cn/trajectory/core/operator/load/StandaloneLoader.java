package whu.edu.cn.trajectory.core.operator.load;

import geotrellis.raster.render.png.PaethFilter;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.data.IDataConfig;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.conf.load.ILoadConfig;
import whu.edu.cn.trajectory.core.conf.load.StandaloneLoadConfig;
import whu.edu.cn.trajectory.core.operator.transform.ParquetTransform;
import whu.edu.cn.trajectory.core.operator.transform.source.*;

import javax.ws.rs.NotSupportedException;
import java.io.File;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StandaloneLoader implements ILoader {

  public List<Trajectory> loadTrajectory(ILoadConfig loadConfig, IDataConfig dataConfig) {
    return null;
  }

  public JavaRDD<Trajectory> loadTrajectory(
      SparkSession sparkSession, ILoadConfig loadConfig, IDataConfig dataConfig) {
    if (loadConfig instanceof StandaloneLoadConfig && dataConfig instanceof TrajectoryConfig) {
      StandaloneLoadConfig standaloneLoadConfig = (StandaloneLoadConfig) loadConfig;
      TrajectoryConfig trajectoryConfig = (TrajectoryConfig) dataConfig;
      switch (standaloneLoadConfig.getFileMode()) {
        case MULTI_FILE:
          return this.loadTrajectoryFromMultiFile(
              sparkSession, standaloneLoadConfig, trajectoryConfig);
        case SINGLE_FILE:
          return this.loadTrajectoryFromSingleFile(
              sparkSession, standaloneLoadConfig, trajectoryConfig);
        default:
          throw new NotSupportedException(
              "can't support fileMode " + standaloneLoadConfig.getFileMode().getMode());
      }
    } else {
      throw new RuntimeException(
          "loadConfig is not a StandAloneLoadConfig or dataConfig is not a TrajectoryConfig in configuration json file");
    }
  }

  @Override
  public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig) {
    throw new NotImplementedError();
  }

  private JavaRDD<Trajectory> loadTrajectoryFromParquet(
      SparkSession sparkSession,
      StandaloneLoadConfig standaloneLoadConfig,
      TrajectoryConfig trajectoryConfig,
      boolean isTraj) {

    String filterText = standaloneLoadConfig.getFilterText();
    String location = standaloneLoadConfig.getLocation();
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
                    file.getAbsolutePath(),
                    sparkSession,
                    filterText,
                    standaloneLoadConfig.getPartNum());
          } else {
            trajectoryJavaRDD =
                ParquetTransform.loadParquetPointData(
                    file.getAbsolutePath(),
                    sparkSession,
                    filterText,
                    trajectoryConfig,
                    standaloneLoadConfig.getPartNum());
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
      SparkSession sparkSession,
      StandaloneLoadConfig standaloneLoadConfig,
      TrajectoryConfig trajectoryConfig) {

    int partNum = standaloneLoadConfig.getPartNum();
    FileTypeEnum fileType = standaloneLoadConfig.getFileType();
    if (fileType == FileTypeEnum.parquet) {
      return loadTrajectoryFromParquet(sparkSession, standaloneLoadConfig, trajectoryConfig, false);
    }
    JavaRDD<Tuple2<String, String>> loadRDD =
        sparkSession
            .sparkContext()
            .wholeTextFiles(standaloneLoadConfig.getLocation(), partNum)
            .toJavaRDD()
            .filter(
                (s) -> {
                  // 过滤空文件
                  return !((String) s._2).isEmpty();
                });
    JavaRDD<Trajectory> resultRdd = null;
    switch (fileType) {
      case csv:
        resultRdd =
            loadRDD
                .map(
                    (s) -> {
                      // 解析、映射为Trajectory
                      Trajectory trajectory =
                          CSV2Traj.multifileParse(
                              s._2(), trajectoryConfig, standaloneLoadConfig.getSplitter());
                      if (trajectory != null && trajectoryConfig.getTrajId().getIndex() < 0) {
                        File file = new File(s._1());
                        String fileNameFull = file.getName();
                        trajectory.setTrajectoryID(
                            fileNameFull.substring(0, fileNameFull.lastIndexOf(".")));
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
                .filter(s -> s._1.endsWith(".shp"))
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
            "can't support fileType " + standaloneLoadConfig.getFileType().getFileTypeEnum());
    }
    return resultRdd;
  }

  private JavaRDD<Trajectory> loadTrajectoryFromSingleFile(
      SparkSession sparkSession,
      StandaloneLoadConfig standaloneLoadConfig,
      TrajectoryConfig trajectoryConfig) {
    int partNum = standaloneLoadConfig.getPartNum();
    FileTypeEnum fileType = standaloneLoadConfig.getFileType();
    if (fileType == FileTypeEnum.parquet) {
      return loadTrajectoryFromParquet(sparkSession, standaloneLoadConfig, trajectoryConfig, true);
    }
    JavaRDD<Tuple2<String, String>> loadRDD =
        sparkSession
            .sparkContext()
            .wholeTextFiles(standaloneLoadConfig.getLocation(), partNum)
            .toJavaRDD();
    JavaRDD<Trajectory> resultRdd = null;
    switch (fileType) {
      case csv:
        resultRdd =
            loadRDD
                .flatMap(
                    s -> {
                      return CSV2Traj.singlefileParse(
                              s._2, trajectoryConfig, standaloneLoadConfig.getSplitter())
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
                .filter(s -> s._1.endsWith(".shp"))
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
            "can't support fileType " + standaloneLoadConfig.getFileType().getFileTypeEnum());
    }
    return resultRdd;
  }
}
