package whu.edu.cn.ogc.entity.process;

import java.util.*;

public enum CoverageMediaType {
    GEOTIFF("geotiff"),
    PNG("png"),
    BINARY("binary");
    private final String type;
    CoverageMediaType(String type) {
        this.type = type;
    }
    public String getType(){
        return type;
    }
    private final static Map<String, Integer> priorityMap = new HashMap<>();
    static {
        // 定义每个mediaType的优先级 数字越小，等级越高
        priorityMap.put(GEOTIFF.type, 3);
        priorityMap.put(PNG.type, 2);
        priorityMap.put(BINARY.type, 1);
    }
    public static String sort(List<String> coverageTypeList) {
        coverageTypeList.sort((s1, s2) -> {
            // 使用通用的比较规则
            int p1 = priorityMap.getOrDefault(s1, 0);
            int p2 = priorityMap.getOrDefault(s2, 0);
            return Integer.compare(p2, p1); // 按照优先级从高到低排序
        });
        return coverageTypeList.get(0);
    }
}
