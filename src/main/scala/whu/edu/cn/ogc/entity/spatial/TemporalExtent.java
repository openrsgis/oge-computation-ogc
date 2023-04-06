package whu.edu.cn.ogc.entity.spatial;

import java.util.List;

public class TemporalExtent {
    /**
     * example [["2011-11-11T11:11:11+00:00", null]]
     */
    List<List<String>> interval;

    public TemporalExtent(List<List<String>> interval) {
        this.interval = interval;
    }

    public List<List<String>> getInterval() {
        return interval;
    }

    public void setInterval(List<List<String>> interval) {
        this.interval = interval;
    }
}
