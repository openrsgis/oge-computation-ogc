package whu.edu.cn.ogc.entity.spatial;

import java.util.List;

public class SpatialExtent {
    /**
     * bbox of the image, example: [[ -180,-90,180, 90]]
     */
    List<List<Float>> bbox;

    /**
     * example: http://www.opengis.net/def/crs/OGC/1.3/CRS84
     */
    String crs;

    public SpatialExtent(List<List<Float>> bbox, String crs) {
        this.bbox = bbox;
        this.crs = crs;
    }

    public List<List<Float>> getBbox() {
        return bbox;
    }

    public void setBbox(List<List<Float>> bbox) {
        this.bbox = bbox;
    }

    public String getCrs() {
        return crs;
    }

    public void setCrs(String crs) {
        this.crs = crs;
    }
}
