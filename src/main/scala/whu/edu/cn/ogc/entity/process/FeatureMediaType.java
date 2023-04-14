package whu.edu.cn.ogc.entity.process;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum FeatureMediaType {
    GML("gml"),
    GEOJSON("geojson");
    private final String type;

    FeatureMediaType(String type) {
        this.type = type;
    }
    public String getType(){
        return type;
    }

    private static final Map<String, Integer> priorityMap = new HashMap<>();

    static {
        // 定义每个mediaType的优先级 数字越小，等级越高
        priorityMap.put(FeatureMediaType.GEOJSON.type, 2);
        priorityMap.put(FeatureMediaType.GML.type, 1);
    }

    public static String sort(List<String> featureTypeList) {
        featureTypeList.sort((s1, s2) -> {
            // 使用通用的比较规则
            int p1 = priorityMap.getOrDefault(s1, 0);
            int p2 = priorityMap.getOrDefault(s2, 0);
            return Integer.compare(p2, p1); // 按照优先级从高到低排序
        });
        return featureTypeList.get(0);
    }
}
