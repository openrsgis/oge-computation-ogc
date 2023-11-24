package whu.edu.cn.trajectory.core.operator.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.data.IDataConfig;
import whu.edu.cn.trajectory.core.conf.load.ILoadConfig;
import whu.edu.cn.trajectory.core.conf.load.*;

import java.io.IOException;
import java.io.Serializable;

public interface ILoader extends Serializable {
    JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig, IDataConfig dataConfig);
    JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig) throws IOException;

    static ILoader getLoader(ILoadConfig loadConfig) {
        switch (loadConfig.getInputType()) {
            case STANDALONE:
                if (loadConfig instanceof StandaloneLoadConfig) {
                    return new StandaloneLoader();
                }
            case HDFS:
                if (loadConfig instanceof HDFSLoadConfig) {
                    return new HDFSLoader();
                }
            case HBASE:
                if (loadConfig instanceof HBaseLoadConfig) {
                    Configuration conf = HBaseConfiguration.create();
                    return new HBaseLoader(conf);
                }
            case HIVE:
                if (loadConfig instanceof HiveLoadConfig) {
                    return new HiveLoader();
                }
                throw new NoSuchMethodError();
//      case GEOMESA:
//        if (loadConfig instanceof GeoMesaInputConfig) {
//          return new GeoMesaLoader();
//        }
//        throw new NoSuchMethodError();
            default:
                throw new NotImplementedError();
        }
    }
}
