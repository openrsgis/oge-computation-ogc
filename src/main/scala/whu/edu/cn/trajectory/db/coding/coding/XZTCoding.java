package whu.edu.cn.trajectory.db.coding.coding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import whu.edu.cn.trajectory.base.trajectory.TrajFeatures;
import whu.edu.cn.trajectory.db.coding.sfc.SFCRange;
import whu.edu.cn.trajectory.db.coding.sfc.XZTSFC;
import whu.edu.cn.trajectory.db.datatypes.ByteArray;
import whu.edu.cn.trajectory.db.datatypes.TimeBin;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;
import whu.edu.cn.trajectory.db.enums.TemporalQueryType;
import whu.edu.cn.trajectory.db.enums.TimePeriod;
import whu.edu.cn.trajectory.db.query.basic.condition.TemporalQueryCondition;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static whu.edu.cn.trajectory.db.constant.CodingConstants.*;


/**
 * @author xuqi
 * @date 2023/12/01
 */
public class XZTCoding implements TimeCoding {


    private final int g;
    private final TimePeriod timePeriod;
    private XZTSFC xztsfc;

    public static final int BYTES_NUM = Long.BYTES;

    @SuppressWarnings("checkstyle:StaticVariableName")
    static ZonedDateTime Epoch = ZonedDateTime.ofInstant(Instant.EPOCH, TIME_ZONE);
    private static Logger logger = LoggerFactory.getLogger(XZTCoding.class);

    public XZTCoding() {
        g = MAX_TIME_BIN_PRECISION;
        timePeriod = DEFAULT_TIME_PERIOD;
        xztsfc = XZTSFC.apply(g, timePeriod);
    }

    public void setXztCoding(XZTSFC xztsfc) {
        this.xztsfc = xztsfc;
    }

    public XZTCoding(int g, TimePeriod timePeriod) {
        if (g > MAX_TIME_BIN_PRECISION) {
            logger.error(
                    "Only support time bin precision lower or equal than {}," + " but found precision is {}",
                    MAX_TIME_BIN_PRECISION, g);
        }
        this.g = g;
        this.timePeriod = timePeriod;
        this.xztsfc = XZTSFC.apply(g, timePeriod);
    }

    public long getIndex(ZonedDateTime start, ZonedDateTime end) {
        return getIndex(new TimeLine(start, end));
    }

    /**
     * 将TimeLine转为Long型编码。
     * @param timeLine With time starting point and end point information
     * @return Time coding
     */
    @Override
    public long getIndex(TimeLine timeLine) {
        TimeBin bin = dateToBinnedTime(timeLine.getTimeStart());
        return xztsfc.index(timeLine, bin);
    }

    /**
     * 将TimeLine转换为字节数组型的编码。
     * @param timeLine
     * @return
     */
    public ByteArray index(TimeLine timeLine) {
        ByteBuffer br = ByteBuffer.allocate(BYTES_NUM);
        long index = getIndex(timeLine);
        br.putLong(index);
        return new ByteArray(br);
    }

    public long getElementCode(long xztCode) {
        return xztCode % xztsfc.binElementCnt();
    }

    public TimeBin getTimeBin(long xztCode) {
        return xztsfc.getTimeBin(xztCode);
    }

    @Override
    public List<CodingRange> ranges(TemporalQueryCondition condition) {
        List<SFCRange> indexRangeList = new ArrayList<>(500);
        indexRangeList = xztsfc.ranges(condition.getQueryWindows(), condition.getTemporalQueryType() == TemporalQueryType.CONTAIN);
        return rangesToCodingRange(indexRangeList);
    }

    // public List<CodingRange> rangesMerged(TemporalQueryCondition condition) {
    //   List<TimeIndexRange> indexRangeList = new ArrayList<>(500);
    //   indexRangeList = XZTSFC.ranges(condition.getQueryWindows(), condition.getTemporalQueryType() == TemporalQueryType.CONTAIN);
    //   List<TimeIndexRange> intervalKeyMerge = getIntervalKeyMerge(indexRangeList);
    //   return rangesToCodingRange(intervalKeyMerge);
    // }
    //
    // public List<TimeIndexRange> getIntervalKeyMerge(List<TimeIndexRange> ranges) {
    //   ranges.sort(Comparator.comparing(TimeIndexRange::getLowerXZTCode));
    //   List<TimeIndexRange> result = new ArrayList<>();
    //   TimeIndexRange current = ranges.get(0);
    //   int i = 1;
    //   while (i < ranges.size()) {
    //     TimeIndexRange indexRange = ranges.get(i);
    //     if (indexRange.getTimeBin().equals(current.getTimeBin())
    //         & indexRange.getLowerElementCode() <= current.getUpperElementCode() + g
    //         & indexRange.isContained() == current.isContained()
    //     ) {
    //       // merge the two ranges
    //       current = new TimeIndexRange(current.getLowerElementCode(),
    //           indexRange.getUpperElementCode(), indexRange.getTimeBin(),
    //           false, this);
    //     } else {
    //       // append the last range and set the current range for future merging
    //       result.add(current);
    //       current = indexRange;
    //     }
    //     i += 1;
    //   }
    //   result.add(current);
    //   return result;
    // }

    public List<CodingRange> rangesToCodingRange(List<SFCRange> timeIndexRangeList) {
        List<CodingRange> codingRangeList = new LinkedList<>();
        for (SFCRange timeIndexRange : timeIndexRangeList) {
            CodingRange codingRange = new CodingRange();
            codingRange.concatTimeIndexRange(timeIndexRange);
            codingRangeList.add(codingRange);
        }
        return codingRangeList;
    }

    public XZTSFC getXZTSFC() {
        return xztsfc;
    }

    public TimePeriod getTimePeriod() {
        return timePeriod;
    }

    public Tuple2<Integer, Long> getExtractTimeKeyBytes(ByteArray timeBytes) {
        ByteBuffer byteBuffer1 = timeBytes.toByteBuffer();
        ((Buffer) byteBuffer1).flip();
        long xzt = byteBuffer1.getLong();
        int binID = getTimeBin(xzt).getBinID();
        long elementCode = getElementCode(xzt);
        return new Tuple2<>(binID, elementCode);
    }

    public TimeBin getTrajectoryTimeBin(TrajFeatures features) {
        ZonedDateTime zonedDateTime = features.getStartTime();
        return dateToBinnedTime(zonedDateTime);
    }

    public TimeBin epochSecondToBinnedTime(long time) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
                TIME_ZONE);
        return dateToBinnedTime(zonedDateTime);
    }

    public TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime) {
        int binId = (int) timePeriod.getChronoUnit().between(Epoch, zonedDateTime);
        return new TimeBin(binId, timePeriod);
    }

    /**
     * @param coding Time coding
     * @return Sequences 0 and 1 of time
     */
    public List<Integer> getSequenceCode(long coding) {
        int g = this.g;
        // coding 减去 bin cnt
        coding = getElementCode(coding);
        List<Integer> list = new ArrayList<>(g);
        for (int i = 0; i < g; i++) {
            if (coding <= 0) {
                break;
            }
            long operator = (long) Math.pow(2, g - i) - 1;
            long s = ((coding - 1) / operator);
            list.add((int) s);
            coding = coding - 1L - s * operator;
        }
        return list;
    }

    /**
     * Obtaining Minimum Time Bounding Box Based on Coding Information
     *
     * @param coding  Time coding
     * @return With time starting point and end point information
     */
    public TimeLine getXZTElementTimeLine(long coding) {
        TimeBin timeBin = getTimeBin(coding);
        List<Integer> list = getSequenceCode(coding);
        double timeMin = 0.0;
        double timeMax = 1.0;
        for (Integer integer : list) {
            double timeCenter = (timeMin + timeMax) / 2;
            if (integer == 0) {
                timeMax = timeCenter;
            } else {
                timeMin = timeCenter;
            }
        }
        ZonedDateTime binStartTime = timeBinToDate(timeBin);
        long timeStart = (long) (timeMin * timePeriod.getChronoUnit().getDuration().getSeconds())
                + binStartTime.toEpochSecond();
        long timeEnd = (long) ((timeMax * timePeriod.getChronoUnit().getDuration().getSeconds())
                + binStartTime.toEpochSecond());
        ZonedDateTime startTime = timeToZonedTime(timeStart);
        ZonedDateTime endTime = timeToZonedTime(timeEnd);

        return new TimeLine(startTime, endTime);
    }

    public static ZonedDateTime timeBinToDate(TimeBin binnedTime) {
        long bin = binnedTime.getBinID();
        return binnedTime.getTimePeriod().getChronoUnit().addTo(Epoch, bin);
    }

    public int timeToBin(long time) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), TIME_ZONE);
        return dateToBin(zonedDateTime);
    }

    public int dateToBin(ZonedDateTime zonedDateTime) {
        long binId = timePeriod.getChronoUnit().between(Epoch, zonedDateTime);
        return (int) binId;
    }

    public static ZonedDateTime timeToZonedTime(long time) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time), TIME_ZONE);
    }

}