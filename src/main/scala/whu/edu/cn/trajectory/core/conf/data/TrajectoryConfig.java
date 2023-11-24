package whu.edu.cn.trajectory.core.conf.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajectory.base.trajectory.TrajFeatures;
import whu.edu.cn.trajectory.core.common.field.Field;
import whu.edu.cn.trajectory.core.enums.BasicDataTypeEnum;
import whu.edu.cn.trajectory.core.enums.DataTypeEnum;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class TrajectoryConfig implements IDataConfig {
    @JsonProperty
    private Field trajId;
    @JsonProperty
    private Field objectId;
    @JsonProperty
    private Field trajLog;
    @JsonProperty
    private Field timeList;
    @JsonProperty
    private TrajPointConfig trajPointConfig;
    @JsonProperty
    private TrajFeatures trajectoryFeatures;
    @JsonProperty
    private List<Mapping> trajectoryMetas;
    @JsonProperty
    private String attrIndexable;
    @JsonProperty
    private String pointIndex;
    @JsonProperty
    private String listIndex;

    public TrajectoryConfig() {
        this.trajId = new Field("trajectory_id", BasicDataTypeEnum.STRING);
        this.objectId = new Field("object_id", BasicDataTypeEnum.STRING);
        this.trajLog = new Field("traj_list", BasicDataTypeEnum.STRING);
        this.timeList = new Field("time_list", BasicDataTypeEnum.STRING);
        this.trajPointConfig = new TrajPointConfig();
        this.pointIndex = "";
        this.listIndex = "";
    }

    public Field getTrajId() {
        return this.trajId;
    }

    public Field getObjectId() {
        return this.objectId;
    }

    public TrajFeatures getTrajectoryFeatures() {
        return this.trajectoryFeatures;
    }

    public List<Mapping> getTrajectoryMetas() {
        return this.trajectoryMetas;
    }

    public TrajPointConfig getTrajPointConfig() {
        return trajPointConfig;
    }

    public Field getTrajLog() {
        return this.trajLog;
    }

    public DataTypeEnum getDataType() {
        return DataTypeEnum.TRAJECTORY;
    }

    public String isIndexable() {
        return this.attrIndexable;
    }

    public String getPointIndex() {
        return this.pointIndex;
    }

    public String getListIndex() {
        return this.listIndex;
    }

    public Field getTimeList() {
        return timeList;
    }
}
