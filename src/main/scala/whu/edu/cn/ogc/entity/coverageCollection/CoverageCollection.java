package whu.edu.cn.ogc.entity.coverageCollection;


import whu.edu.cn.ogc.entity.collection.Collection;
import whu.edu.cn.ogc.entity.coverage.Coverage;
import whu.edu.cn.ogc.entity.spatial.Extent;

import java.util.List;

public class CoverageCollection extends Collection {

    private List<Coverage> coverageCollection;

    public CoverageCollection(String productId, int size, String description, String crs, String itemType, Extent extent, List<Coverage> coverageCollection) {
        super(productId, size, description, crs, itemType, extent);
        this.coverageCollection = coverageCollection;
    }

    public List<Coverage> getCoverageCollection() {
        return coverageCollection;
    }

    public void setCoverageCollection(List<Coverage> coverageCollection) {
        this.coverageCollection = coverageCollection;
    }
}
