package whu.edu.cn.trajectory.db.database.util;

import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajectory.base.trajectory.Trajectory;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public abstract class TextTrajParser {

    public abstract Trajectory parse(String line) throws ParseException;
}
