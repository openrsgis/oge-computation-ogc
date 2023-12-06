package whu.edu.cn.trajectory.db.coding.coding;

import whu.edu.cn.trajectory.db.datatypes.TimeBin;
import whu.edu.cn.trajectory.db.datatypes.TimeLine;
import whu.edu.cn.trajectory.db.query.basic.condition.TemporalQueryCondition;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public interface TimeCoding extends Serializable {

    /**
     * Generate time period encoding through timeline
     * 
     * @param timeline timeline
     * @return Time period character encoding
     */
    long getIndex(TimeLine timeline);

    /**
     * The timebin interval to which the generation time belongs
     * @param zonedDateTime time
     * @return The timebin interval to which the time belongs
     */
    TimeBin dateToBinnedTime(ZonedDateTime zonedDateTime);

    /**
     *  Generate timeline bounding boxes corresponding to coding
     * @param coding Time period character encoding
     * @return timeline
     */
    TimeLine getXZTElementTimeLine(long coding);

    /**
     * Parse the time query conditions and return the time encoding interval
     * @param condition Time query criteria
     * @return Time coding interval
     */
    List<CodingRange> ranges(TemporalQueryCondition condition);
}
