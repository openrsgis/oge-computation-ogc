package whu.edu.cn.trajectory.core.conf.load;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public enum FileModeEnum {
    MULTI_FILE("multi_file"),
    SINGLE_FILE("single_file"),
    MULTI_SINGLE_FILE("multi_single_file");

    private final String mode;

    FileModeEnum(String mode) {
        this.mode = mode;
    }

    @JsonValue
    public String getMode() {
        return this.mode;
    }
}
