package whu.edu.cn.trajectory.core.operator.transform;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.point.TrajPointWithoutGeometry;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.base.trajectory.TrajectoryWithoutGeometry;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.base.util.CheckUtils;
import whu.edu.cn.trajectory.base.util.SerializerUtils;
import whu.edu.cn.trajectory.core.conf.data.Mapping;
import whu.edu.cn.trajectory.core.conf.data.TrajPointConfig;
import whu.edu.cn.trajectory.core.conf.data.TrajectoryConfig;
import whu.edu.cn.trajectory.core.util.DataTypeUtils;
import whu.edu.cn.trajectory.example.util.SparkSessionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.*;

import static whu.edu.cn.trajectory.base.trajectory.TrajectoryWithoutGeometry.parseWithoutGeometryToTrajPoint;

/**
 * @author xuqi
 * @date 2023/11/27
 */
public class ParquetTransform {
  public static void parseCSVToParquet(
      String sourcePath, Boolean hasHeader, SparkSession sparkSession, String outputPath) {
    File directory = new File(sourcePath);
    File[] files = directory.listFiles();
    Dataset<Row> unionSet = null;
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          // 读取CSV文件并转换为DataFrame对象
          Dataset<Row> csvData =
              sparkSession.read().format("csv").option("header", hasHeader).load(sourcePath);
          if (unionSet == null) {
            unionSet = csvData;
          } else {
            unionSet.union(csvData);
          }
        }
      }
    }

    // 将DataFrame对象保存为Parquet文件
    assert unionSet != null;
    unionSet.write().format("parquet").mode("overwrite").save(outputPath);
  }

  public static JavaRDD<Trajectory> loadParquetPointData(
      String sourcePath,
      SparkSession sparkSession,
      String filterText,
      TrajectoryConfig trajectoryConfig,
      int numPartitions) {
    // 读取Parquet文件并转换为DataFrame对象
    Dataset<Row> parquetData =
        sparkSession
            .read()
            .format("parquet")
            .option("numPartitions", numPartitions)
            .load(sourcePath);
    Dataset<Row> selectedData = parquetData;
    // 对Parquet数据进行操作，例如选择和过滤
    if (filterText != null) {
      selectedData = parquetData.select("*").filter(filterText);
    }
    // 按tid+oid分组
    int objectIdIndex = trajectoryConfig.getObjectId().getIndex();
    int trajIdIndex = trajectoryConfig.getTrajId().getIndex();

    JavaPairRDD<String, Iterable<Row>> iterableJavaPairRDD =
        selectedData.toJavaRDD().groupBy(item -> getGroupKey(item, trajIdIndex, objectIdIndex));
    // 将DataFrame对象转换为JavaRDD<Trajectory>对象并返回

    return iterableJavaPairRDD.map(
        item -> mapToTraj(item._2, trajIdIndex, objectIdIndex, trajectoryConfig));
  }

  public static JavaRDD<Trajectory> loadParquetTrajData(
      String sourcePath, SparkSession sparkSession, String filterText, int numPartitions) {
    // 读取Parquet文件并转换为DataFrame对象
    Dataset<Row> parquetData =
        sparkSession
            .read()
            .format("parquet")
            .option("numPartitions", numPartitions)
            .load(sourcePath);
    Dataset<Row> selectedData = parquetData;
    // 对Parquet数据进行操作，例如选择和过滤
    if (filterText != null) {
      selectedData = parquetData.select("*").filter(filterText);
    }

    // 将数据转换为自定义类 Trajectory
    return selectedData.toJavaRDD().map(ParquetTransform::mapToTrajectory);
  }

  private static String getGroupKey(Row row, int trajIdIndex, int objectIdIndex) {
    return row.getString(trajIdIndex) + "#" + row.getString(objectIdIndex);
  }

  private static Trajectory mapToTraj(
      Iterable<Row> points, int trajIdIndex, int objectIdIndex, TrajectoryConfig trajectoryConfig)
      throws IOException {
    String trajId = "", objectId = "";
    List<TrajPoint> trajPoints = new ArrayList<>();
    for (Row next : points) {
      trajId = next.getString(trajIdIndex);
      objectId = next.getString(objectIdIndex);
      TrajPoint trajPoint = mapToPoint(next, trajectoryConfig.getTrajPointConfig());
      trajPoints.add(trajPoint);
    }
    if (!trajPoints.isEmpty()) {
      trajPoints.sort(
          (o1, o2) -> {
            return (int) (o1.getTimestamp().toEpochSecond() - o2.getTimestamp().toEpochSecond());
          });
      return new Trajectory(trajId, objectId, trajPoints);
    } else {
      return null;
    }
  }

  private static TrajPoint mapToPoint(Row row, TrajPointConfig config) throws IOException {
    try {
      int pidIdx = config.getPointId().getIndex();
      String id;
      if (pidIdx < 0) {
        id = null;
      } else {
        id = row.getString(config.getPointId().getIndex());
      }
      ZonedDateTime time =
          BasicDateUtils.parse(
              config.getTime().getBasicDataTypeEnum(),
              row.getString(config.getTime().getIndex()),
              config.getTime());
      double lat = Double.parseDouble(row.getString(config.getLat().getIndex()));
      double lng = Double.parseDouble(row.getString(config.getLng().getIndex()));
      int n = row.size();
      Map<String, Object> metas = new HashMap<>();
      if (config.getTrajPointMetas() != null) {
        // 对于明确指定映射名的列，赋予映射名，其他列顺序赋映射名
        List<Mapping> trajPointMetas = config.getTrajPointMetas();
        int j = 0;
        for (int i = 0; i < n; ++i) {
          if (j < trajPointMetas.size() && i == trajPointMetas.get(j).getIndex()) {
            Mapping m = trajPointMetas.get(j);
            metas.put(
                m.getMappingName(),
                DataTypeUtils.parse(
                    row.getString(m.getIndex()), m.getDataType(), m.getSourceData()));
            j++;
          } else {
            metas.put(String.valueOf(i), row.get(i));
          }
        }
      } else {
        // 无明确指定，则顺序生成映射名
        for (int i = 0; i < n; ++i) {
          metas.put(String.valueOf(i), row.get(i));
        }
      }
      return new TrajPoint(id, time, lng, lat, metas);
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
  }

  public static Trajectory mapToTrajectory(Row row) throws IOException {
    String oid = row.getAs("objectID");
    String tid = row.getAs("trajectoryID");
    List<TrajPoint> trajPoints =
        parseWithoutGeometryToTrajPoint(
            SerializerUtils.deserializeList(
                    row.getAs("pointList"), TrajPointWithoutGeometry.class));
    return new Trajectory(tid, oid, trajPoints);
  }

  public static void storeTrajAsParquet(
      JavaRDD<Trajectory> trajectoryJavaRDD, SparkSession sparkSession, String outputPath) {
    // 将示例数据转换为 DataFrame
    JavaRDD<TrajectoryWithoutGeometry> trajectoryWithoutGeometryJavaRDD =
        trajectoryJavaRDD.map(
            t -> {
              return new TrajectoryWithoutGeometry(
                  t.getTrajectoryID(), t.getObjectID(), t.getPointList());
            });
    Dataset<Row> dataFrame =
        sparkSession.createDataFrame(
            trajectoryWithoutGeometryJavaRDD, TrajectoryWithoutGeometry.class);

    // 将 DataFrame 写入为 Parquet 文件
    dataFrame.write().mode("overwrite").parquet(outputPath);
  }
}
