package whu.edu.cn.ogc.entity.collections;

import whu.edu.cn.ogc.entity.coverageCollection.CoverageCollection;
import whu.edu.cn.ogc.entity.featureCollection.FeatureCollection;

import java.util.List;

public class Collections {

    private List<CoverageCollection> coverageCollectionList;
    private List<FeatureCollection> featureCollectionList;

    public List<CoverageCollection> getCoverageCollectionList() {
        return coverageCollectionList;
    }

    public void setCoverageCollectionList(List<CoverageCollection> coverageCollectionList) {
        this.coverageCollectionList = coverageCollectionList;
    }

    public List<FeatureCollection> getFeatureCollectionList() {
        return featureCollectionList;
    }

    public void setFeatureCollectionList(List<FeatureCollection> featureCollectionList) {
        this.featureCollectionList = featureCollectionList;
    }
}
