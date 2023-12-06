package whu.edu.cn.trajectory.db.database.meta;

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajectory.base.point.TrajPoint;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;
import whu.edu.cn.trajectory.base.util.BasicDateUtils;
import whu.edu.cn.trajectory.db.constant.SetConstants;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;

import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static whu.edu.cn.trajectory.base.util.GeoUtils.getEuclideanDistanceKM;

/**
 * @author xuqi
 * @date 2023/12/05
 */
public class SetMeta {
  private static Logger logger = LoggerFactory.getLogger(SetMeta.class);
  private ZonedDateTime start_time = BasicDateUtils.parseDate(SetConstants.start_time);
  private int srid = SetConstants.srid;
  private MinimumBoundingBox boundingBox;
  private TimeLine timeLine;
  private int dataCount;

    public SetMeta(JavaRDD<Trajectory> trajectoryJavaRDD) {
        this.buildSetMetaFromRDD(trajectoryJavaRDD);
    }

    public SetMeta(
      ZonedDateTime start_time,
      int srid,
      MinimumBoundingBox boundingBox,
      TimeLine timeLine,
      int dataCount) {
    this.start_time = start_time;
    this.srid = srid;
    this.boundingBox = boundingBox;
    this.timeLine = timeLine;
    this.dataCount = dataCount;
  }

  public SetMeta(MinimumBoundingBox boundingBox, TimeLine timeLine, int dataCount) {
    this.boundingBox = boundingBox;
    this.timeLine = timeLine;
    this.dataCount = dataCount;
  }


  public static Logger getLogger() {
    return logger;
  }

  public ZonedDateTime getStart_time() {
    return start_time;
  }

  public int getSrid() {
    return srid;
  }

  public MinimumBoundingBox getBoundingBox() {
    return boundingBox;
  }

  public TimeLine getTimeLine() {
    return timeLine;
  }

  public int getDataCount() {
    return dataCount;
  }

  public void buildSetMetaFromRDD(JavaRDD<Trajectory> trajectoryJavaRDD) {
    trajectoryJavaRDD.foreachPartition(
        t -> {
          if (t.hasNext()) {
            t.next().getTrajectoryFeatures();
          }
        });
    // 创建AtomicReference对象，初始值为全局box对象
    AtomicReference<MinimumBoundingBox> boxRef = new AtomicReference<>(null);
    AtomicReference<ZonedDateTime> start_timeRef = new AtomicReference<>(null);
    AtomicReference<ZonedDateTime> end_timeRef = new AtomicReference<>(null);
    AtomicReference<Integer> dataCountRef = new AtomicReference<>(0);

    // 对RDD进行foreachPartition操作
    trajectoryJavaRDD.foreachPartition(
        partitionItr -> {
          // 创建一个临时的局部box对象
          MinimumBoundingBox localBox = null;
          ZonedDateTime start_time = null;
          ZonedDateTime end_time = null;
          int count = 0;
          while (partitionItr.hasNext()) {
            Trajectory t = partitionItr.next();
            if (localBox == null) {
              localBox = t.getTrajectoryFeatures().getMbr();
              start_time = t.getTrajectoryFeatures().getStartTime();
              end_time = t.getTrajectoryFeatures().getEndTime();
              count = t.getTrajectoryFeatures().getPointNum();
            } else {
              localBox = localBox.union(t.getTrajectoryFeatures().getMbr());
              start_time =
                  start_time.compareTo(t.getTrajectoryFeatures().getStartTime()) <= 0
                      ? start_time
                      : t.getTrajectoryFeatures().getStartTime();
              end_time =
                  end_time.compareTo(t.getTrajectoryFeatures().getEndTime()) >= 0
                      ? end_time
                      : t.getTrajectoryFeatures().getEndTime();
              count += t.getTrajectoryFeatures().getPointNum();
            }
          }
          // 将局部box对象合并到全局box对象中
          MinimumBoundingBox finalLocalBox = localBox;
          ZonedDateTime finalStartTime = start_time;
          ZonedDateTime finalEndTime = end_time;
          int finalCount = count;
          boxRef.getAndUpdate(
              currentBox -> {
                if (currentBox == null) return finalLocalBox;
                else {
                  return currentBox.union(finalLocalBox);
                }
              });
          start_timeRef.getAndUpdate(
              s -> {
                if (s == null) {
                  return finalStartTime;
                } else {
                  return s.compareTo(finalStartTime) <= 0 ? s : finalStartTime;
                }
              });
          end_timeRef.getAndUpdate(
              s -> {
                if (s == null) {
                  return finalEndTime;
                } else {
                  return s.compareTo(finalEndTime) >= 0 ? s : finalEndTime;
                }
              });
          dataCountRef.getAndUpdate(d -> d + finalCount);
        });
    this.boundingBox = boxRef.get();
    this.timeLine = new TimeLine(start_timeRef.get(), end_timeRef.get());
    this.dataCount = dataCountRef.get();
  }
}
