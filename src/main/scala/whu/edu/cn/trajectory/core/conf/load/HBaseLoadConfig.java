package whu.edu.cn.trajectory.core.conf.load;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HBaseLoadConfig implements ILoadConfig {

    private final String dataSetName;
    private byte[] SCAN_ROW_START;
    private byte[] SCAN_ROW_STOP;
    private String scanRowStart;
    private String scanRowEnd;


    @JsonCreator
    public HBaseLoadConfig(@JsonProperty("dataSetName") String dataSetName,
                           @JsonProperty("SCAN_ROW_START") @JsonInclude(JsonInclude.Include.NON_NULL) String scanRowStart,
                           @JsonProperty("SCAN_ROW_STOP") @JsonInclude(JsonInclude.Include.NON_NULL) String scanRowEnd

    ) {
        this.dataSetName = dataSetName;
        this.scanRowStart = scanRowStart;
        this.scanRowEnd = scanRowEnd;
        init();
    }
    public void init(){
        if(scanRowEnd != null && scanRowStart != null){
            this.SCAN_ROW_START = Bytes.toBytes(scanRowStart);
            this.SCAN_ROW_STOP = Bytes.toBytes(scanRowEnd);
        }
    }

    public String getDataSetName() {
        return dataSetName;
    }

    public byte[] getSCAN_ROW_START() {
        return SCAN_ROW_START;
    }

    public void setSCAN_ROW_START(byte[] SCAN_ROW_START) {
        this.SCAN_ROW_START = SCAN_ROW_START;
    }

    public byte[] getSCAN_ROW_STOP() {
        return SCAN_ROW_STOP;
    }

    public void setSCAN_ROW_STOP(byte[] SCAN_ROW_STOP) {
        this.SCAN_ROW_STOP = SCAN_ROW_STOP;
    }

    @Override
    public InputType getInputType() {
        return InputType.HBASE;
    }

    @Override
    public String getFsDefaultName() {
        return null;
    }
}
