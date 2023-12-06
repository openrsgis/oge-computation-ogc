package whu.edu.cn.trajectory.db.coding.sfc;

/**
 * @author xuqi
 * @date 2023/11/29
 */
public class CoveredSFCRange extends SFCRange{

    public CoveredSFCRange(long lower, long upper) {
        super(lower, upper, true);
    }
}
