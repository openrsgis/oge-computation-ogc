package whu.edu.cn.trajectory.example.transform;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.operator.load.ILoader;
import whu.edu.cn.trajectory.core.operator.store.IStore;
import whu.edu.cn.trajectory.core.operator.transform.sink.Traj2Shp;
import whu.edu.cn.trajectory.core.util.IOUtils;
import whu.edu.cn.trajectory.example.conf.ExampleConfig;
import whu.edu.cn.trajectory.example.util.SparkSessionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class TrajToShp {
    private static final Logger LOGGER = LoggerFactory.getLogger(TrajToShp.class);

    @Test
    public void TrajToShpFromText() throws IOException {
        String inPath =
                Objects.requireNonNull(TrajToWKT.class.getResource("/ioconf/LoadConfig.json"))
                        .getPath();
        String outPath = "D:/bigdata/oge-computation-ogc/src/main/resources/outfiles/geojson/trans.shp";
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
            List<Trajectory> trajectories = featuresJavaRDD.collect();
            Traj2Shp.createShapefile(outPath, trajectories);
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void TrajToShpFromConf() throws IOException {
        String inPath =
                Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/store/ShapeStore.json"))
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
            IStore iStore = IStore.getStore(exampleConfig.getStoreConfig());
            iStore.storeTrajectory(featuresJavaRDD);
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
