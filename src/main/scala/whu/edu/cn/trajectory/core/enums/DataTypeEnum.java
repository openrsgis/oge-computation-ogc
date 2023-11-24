package whu.edu.cn.trajectory.core.enums;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public enum DataTypeEnum implements Serializable {
    TRAJ_POINT("traj_point"),
    TRAJECTORY("trajectory"),
    MBR("mbr");

    private String dataType;

    DataTypeEnum(String dataType) {
        this.dataType = dataType;
    }

    public static class Constants {
        public static final String TRAJ_POINT = "traj_point";
        public static final String TRAJECTORY = "trajectory";
        public static final String MBR = "mbr";

        public Constants() {
        }
    }
}

