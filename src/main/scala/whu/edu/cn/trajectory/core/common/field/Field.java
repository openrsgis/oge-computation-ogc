package whu.edu.cn.trajectory.core.common.field;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajectory.core.enums.BasicDataTypeEnum;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class Field implements Serializable {
    private String sourceName;
    private BasicDataTypeEnum basicDataTypeEnum;
    private int index;
    private String format;
    private String zoneId;
    private String unit;

    @JsonCreator
    public Field(@JsonProperty("sourceName") String sourceName, @JsonProperty("dataType") BasicDataTypeEnum basicDataTypeEnum, @JsonProperty("index") @JsonInclude(JsonInclude.Include.NON_NULL) int index, @JsonProperty("format") @JsonInclude(JsonInclude.Include.NON_NULL) String format, @JsonProperty("zoneId") @JsonInclude(JsonInclude.Include.NON_NULL) String zoneId, @JsonProperty("unit") @JsonInclude(JsonInclude.Include.NON_NULL) String unit) {
        this.sourceName = sourceName;
        this.basicDataTypeEnum = basicDataTypeEnum;
        this.index = index;
        this.format = format;
        this.zoneId = zoneId;
        this.unit = unit;
    }

    public Field() {
    }

    public Field(String sourceName, BasicDataTypeEnum basicDataTypeEnum) {
        this.sourceName = sourceName;
        this.basicDataTypeEnum = basicDataTypeEnum;
    }

    public Field(String sourceName, BasicDataTypeEnum basicDataTypeEnum, int index) {
        this.sourceName = sourceName;
        this.basicDataTypeEnum = basicDataTypeEnum;
        this.index = index;
    }

    public String getSourceName() {
        return this.sourceName;
    }

    public BasicDataTypeEnum getBasicDataTypeEnum() {
        return this.basicDataTypeEnum;
    }

    public int getIndex() {
        return this.index;
    }

    public String getFormat() {
        return this.format;
    }

    public String getZoneId() {
        return this.zoneId;
    }

    public String getUnit() {
        return this.unit;
    }
}
