package whu.edu.cn.trajectory.core.enums;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author xuqi
 * @date 2023/11/20
 */
public enum FileTypeEnum {
    csv("csv"),
    geojson("geojson"),
    wkt("wkt"),
    kml("kml"),
    shp("shp");
    private final String fileType;

    FileTypeEnum(String fileType) {
        this.fileType = fileType;
    }

    @JsonValue
    public String getFileTypeEnum() {
        return this.fileType;
    }
}
