package whu.edu.cn.trajectory.core.operator.store;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajectory.base.point.StayPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.store.HBaseStoreConfig;
import whu.edu.cn.trajectory.core.conf.store.HiveStoreConfig;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HiveStore implements IStore{
    //TODO hive store
    private final HiveStoreConfig storeConfig;

    public HiveStore(HiveStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    @Override
    public void storeTrajectory(JavaRDD<Trajectory> t) throws Exception {

    }

    @Override
    public void storeTrajectory(JavaRDD<Trajectory> t, SparkSession ss) throws Exception {

    }

    @Override
    public void storeStayPointList(JavaRDD<List<StayPoint>> spList) {

    }

    @Override
    public void storeStayPointASTraj(JavaRDD<StayPoint> sp) {

    }
}
