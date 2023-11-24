package whu.edu.cn.trajectory.core.conf.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajectory.core.enums.FileTypeEnum;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class StandaloneLoadConfig implements ILoadConfig {
    private String master;
    private String location;
    private FileModeEnum fileModeEnum;
    private int partNum;
    private String splitter;
    private FileTypeEnum fileType;

  @JsonCreator
  public StandaloneLoadConfig(
      @JsonProperty("master") String master,
      @JsonProperty("location") String location,
      @JsonProperty("fileMode") FileModeEnum fileModeEnum,
      @JsonProperty("partNum") @JsonInclude(JsonInclude.Include.NON_NULL) int partNum,
      @JsonProperty("splitter") String splitter,
      @JsonProperty("fileType") FileTypeEnum fileType) {
        this.master = master;
        this.location = location;
        this.fileModeEnum = fileModeEnum;
        this.partNum = partNum;
        this.splitter = splitter;
        this.fileType = fileType;
    }

    public String getMaster() {
        return this.master;
    }

    public FileTypeEnum getFileType() {
        return fileType;
    }

    public int getPartNum() {
        return this.partNum == 0 ? 1 : this.partNum;
    }

    public String getLocation() {
        return this.location;
    }

    public FileModeEnum getFileMode() {
        return this.fileModeEnum;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public ILoadConfig.InputType getInputType() {
        return InputType.STANDALONE;
    }

    @Override
    public String getFsDefaultName() {
        return null;
    }

    public String getSplitter() {
        return this.splitter;
    }


}