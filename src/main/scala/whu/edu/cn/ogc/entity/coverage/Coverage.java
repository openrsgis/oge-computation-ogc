package whu.edu.cn.ogc.entity.coverage;

import com.alibaba.fastjson.JSONObject;
import whu.edu.cn.ogc.entity.item.Item;
import whu.edu.cn.ogc.entity.process.Link;
import whu.edu.cn.ogc.entity.spatial.SpatialExtent;

import java.util.List;

public class Coverage extends Item {

    private SpatialExtent spatialExtent;
    /**
     * domain set (not necessary)
     */
    private JSONObject domainSet;
    /**
     * range type (not necessary)
     */
    private JSONObject rangeType;
    /**
     * the download url of the coverage (must)
     */
    private List<Link> coverageLinks;


    public Coverage(String id, List<String> crs, String timeStamp, String itemType, SpatialExtent spatialExtent, JSONObject domainSet, JSONObject rangeType, List<Link> coverageLinks) {
        super(id, crs, timeStamp, itemType);
        this.spatialExtent = spatialExtent;
        this.domainSet = domainSet;
        this.rangeType = rangeType;
        this.coverageLinks = coverageLinks;
    }

    public SpatialExtent getSpatialExtent() {
        return spatialExtent;
    }

    public void setSpatialExtent(SpatialExtent spatialExtent) {
        this.spatialExtent = spatialExtent;
    }

    public JSONObject getDomainSet() {
        return domainSet;
    }

    public void setDomainSet(JSONObject domainSet) {
        this.domainSet = domainSet;
    }

    public JSONObject getRangeType() {
        return rangeType;
    }

    public void setRangeType(JSONObject rangeType) {
        this.rangeType = rangeType;
    }

    public List<Link> getCoverageLinks() {
        return coverageLinks;
    }

    public void setCoverageLinks(List<Link> coverageLinks) {
        this.coverageLinks = coverageLinks;
    }
}
