package whu.edu.cn.trajectory.example.transform;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.operator.load.ILoader;
import whu.edu.cn.trajectory.core.operator.transform.source.GeoJson2Traj;
import whu.edu.cn.trajectory.core.util.IOUtils;
import whu.edu.cn.trajectory.example.conf.ExampleConfig;
import whu.edu.cn.trajectory.example.util.SparkSessionUtils;

import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/16
 */
public class GeojsonToTraj {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeojsonToTraj.class);
  @Test
  public void GeojsonToTrajFromConf() throws JsonParseException {
    String inPath =
            Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/load/GeojsonLoad.json"))
                    .getPath();
    String fileStr = IOUtils.readLocalTextFile(inPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    try (SparkSession sparkSession =
                 SparkSessionUtils.createSession(
                         exampleConfig.getLoadConfig(), TrajToGeojson.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
              iLoader.loadTrajectory(
                      sparkSession, exampleConfig.getLoadConfig(), exampleConfig.getDataConfig());
      JavaRDD<Trajectory> featuresJavaRDD =
              trajRDD.map(
                      trajectory -> {
                        trajectory.getTrajectoryFeatures();
                        return trajectory;
                      });
      System.out.println(featuresJavaRDD.take(1));
      LOGGER.info("Finished!");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
