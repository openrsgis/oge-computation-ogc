package whu.edu.cn.trajectory.db.enums;

/**
 * @author xuqi
 * @date 2023/11/30
 */
public enum TemporalQueryType {
    /**
     * Query all data that may contained with query window.
     */
    CONTAIN,
    /**
     * Query all data that is totally INTERSECT in query window.
     */
    INTERSECT;
}
