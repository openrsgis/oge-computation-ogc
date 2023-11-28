package whu.edu.cn.trajectory.core.operator.store;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.point.StayPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.core.conf.store.HDFSStoreConfig;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;
import whu.edu.cn.trajectory.core.operator.transform.sink.*;
import whu.edu.cn.trajectory.core.util.IOUtils;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HDFSStore implements IStore {
    private static final Logger LOGGER = Logger.getLogger(HDFSStore.class);
    private HDFSStoreConfig storeConfig;

    HDFSStore(HDFSStoreConfig storeConfig) {
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
        switch (fileType){
            case csv:
                JavaPairRDD<String, String> cachedRDD = trajectoryJavaRDD.mapToPair(
                        item -> {
                            String fileName;
                            if (item.getObjectID() == null) {
                                fileName =
                                        String.format("%s%s",
                                                item.getTrajectoryID(),
                                                storeConfig.getFilePostFix());
                            } else {
                                fileName =
                                        String.format("%s/%s%s",
                                                item.getObjectID(),
                                                item.getTrajectoryID(),
                                                storeConfig.getFilePostFix());
                            }
                            String outputString = Traj2CSV.convert(item, storeConfig.getSplitter());
                            return new Tuple2<>(fileName, outputString);
                        }
                ).persist(StorageLevels.MEMORY_AND_DISK);
                Map<String, Long> keyCountedResult = cachedRDD.countByKey();
                cachedRDD.partitionBy(new HashPartitioner(keyCountedResult.size()))
                        .saveAsHadoopFile(storeConfig.getLocation(), String.class, String.class,
                                RDDMultipleTextOutputFormat.class);
                cachedRDD.unpersist();
                break;
            case wkt:
                String wktStr = Traj2WKT.convertTrajListToWKT(trajectoryList);
                IOUtils.writeStringToHdfsFile(fileStoreName, wktStr);
                break;
            case geojson:
                JSONObject jsonObject = Traj2GeoJson.convertTrajListToGeoJson(trajectoryList);
                IOUtils.writeStringToHdfsFile(fileStoreName, jsonObject.toString());
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
    public void storeTrajectory(JavaRDD<Trajectory> t, SparkSession ss) throws Exception {

    }


    @Override
    public void storeStayPointList(JavaRDD<List<StayPoint>> spList) {
    }

    @Override
    public void storeStayPointASTraj(JavaRDD<StayPoint> sp) {
    }

    public void storeStayPoint(JavaRDD<StayPoint> stayPointJavaRDD) {
        stayPointJavaRDD.mapToPair((stayPoint) -> {
                    String record =
                            stayPoint.getSid() + "," + stayPoint.getOid() + "," + stayPoint.getStartTime() + ","
                                    + stayPoint.getEndTime() + ",'" + stayPoint.getCenterPoint() + "'";
                    return new Tuple2(stayPoint.getSid(), record);
                }).persist(StorageLevels.MEMORY_AND_DISK)
                .saveAsHadoopFile(this.storeConfig.getLocation(), String.class, String.class,
                        RDDMultipleTextOutputFormat.class);
    }

    public static class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat<String, String> {
        public RDDMultipleTextOutputFormat() {
        }

        public String generateFileNameForKeyValue(String key, String value, String name) {
            return key;
        }

        protected String generateActualKey(String key, String value) {
            return super.generateActualKey(null, value);
        }
    }
}
