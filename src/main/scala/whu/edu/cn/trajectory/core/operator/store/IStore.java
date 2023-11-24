package whu.edu.cn.trajectory.core.operator.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import scala.NotImplementedError;
import whu.edu.cn.trajectory.base.point.StayPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.store.*;

import java.io.Serializable;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public interface IStore extends Serializable {
    void storeTrajectory(JavaRDD<Trajectory> t) throws Exception;
    void storeStayPointList(JavaRDD<List<StayPoint>> spList);
    void storeStayPointASTraj(JavaRDD<StayPoint> sp);

    static IStore getStore(IStoreConfig storeConfig) {
        switch (storeConfig.getStoreType()) {
            case HDFS:
                if (storeConfig instanceof HDFSStoreConfig) {
                    return new HDFSStore((HDFSStoreConfig) storeConfig);
                }
            case STANDALONE:
                if (storeConfig instanceof StandaloneStoreConfig) {
                    return new StandaloneStore((StandaloneStoreConfig) storeConfig);
                }
            case HBASE:
                if (storeConfig instanceof HBaseStoreConfig) {
                    Configuration conf = HBaseConfiguration.create();
                    return new HBaseStore((HBaseStoreConfig) storeConfig, conf);
                }
            case HIVE:
                if (storeConfig instanceof HiveStoreConfig) {
                    return new HiveStore((HiveStoreConfig) storeConfig);
                }
                throw new NoSuchMethodError();
            default:
                throw new NotImplementedError();
        }
    }
}
