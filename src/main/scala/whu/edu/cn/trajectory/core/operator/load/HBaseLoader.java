package whu.edu.cn.trajectory.core.operator.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.data.IDataConfig;
import whu.edu.cn.trajectory.core.conf.load.HBaseLoadConfig;
import whu.edu.cn.trajectory.core.conf.load.ILoadConfig;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HBaseLoader extends Configured implements ILoader {
    public HBaseLoader(Configuration conf) {

    }

    @Override
    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig, IDataConfig dataConfig) {
        return null;
    }

    @Override
    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig) throws IOException {
        return null;
    }

//    private static final Logger LOGGER = Logger.getLogger(HBaseLoader.class);
//    private static Database instance;
//
//    static {
//        try {
//            instance = Database.getInstance();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public HBaseLoader(Configuration conf) {
//        this.setConf(conf);
//    }
//
//    private void initLoader(ILoadConfig loadConfig, Configuration conf) throws IOException {
//        if (loadConfig instanceof HBaseLoadConfig) {
//            String dataSetName = ((HBaseLoadConfig) loadConfig).getDataSetName();
//            DataSetMeta dataSetMeta = instance.getDataSetMeta(dataSetName);
//            conf.set(TableInputFormat.INPUT_TABLE,
//                    dataSetMeta.getCoreIndexMeta().getIndexTableName());
//            if (((HBaseLoadConfig) loadConfig).getSCAN_ROW_START() != null) {
//                conf.set(TableInputFormat.SCAN_ROW_START,
//                        new String(((HBaseLoadConfig) loadConfig).getSCAN_ROW_START()));
//            }
//            if (((HBaseLoadConfig) loadConfig).getSCAN_ROW_STOP() != null) {
//                conf.set(TableInputFormat.SCAN_ROW_STOP,
//                        new String(((HBaseLoadConfig) loadConfig).getSCAN_ROW_STOP()));
//            }
////      conf.set(TableInputFormat.SCAN_BATCHSIZE, "100");
//        }
//    }
//
//    @Override
//    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig,
//                                              IDataConfig dataConfig) {
//        return null;
//    }
//
//    @Override
//    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig)
//            throws IOException {
//        LOGGER.info("Start loading data from HBase");
//        initLoader(loadConfig, getConf());
//        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRdd = ss.sparkContext()
//                .newAPIHadoopRDD(getConf(), TableInputFormat.class, ImmutableBytesWritable.class,
//                        Result.class).toJavaRDD();
//        JavaRDD<Trajectory> trajectoryJavaRDD = hbaseRdd
//                .map((resultTuple2) -> {
//                    try {
//                        return TrajectoryDataMapper.mapHBaseResultToTrajectory(resultTuple2._2);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//        LOGGER.info(
//                "Successfully load dataSet from HBase named " + ((HBaseLoadConfig) loadConfig).getDataSetName());
//        return trajectoryJavaRDD;
//    }
}
