package whu.edu.cn.trajectory.core.enums;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public enum FileSplitterEnum implements Serializable {
    /**
     * The csv.
     */
    CSV(","),

    /**
     * The tsv.
     */
    TSV("\t"),

    /**
     * The geojson.
     */
    GEOJSON(""),

    /**
     * The wkt.
     */
    WKT("\t"),

    /**
     * The wkb.
     */
    WKB("\t"),

    COMMA(","),

    TAB("\t"),

    QUESTIONMARK("?"),

    SINGLEQUOTE("\'"),

    QUOTE("\""),

    UNDERSCORE("_"),

    DASH("-"),

    PERCENT("%"),

    TILDE("~"),

    PIPE("|"),

    SEMICOLON(";");

    /**
     * The splitter.
     */
    private final String splitter;

    // A lookup map for getting a FileDataSplitter from a delimiter, or its name
    @SuppressWarnings("checkstyle:ConstantName")
    private static final Map<String, FileSplitterEnum> lookupMap =
            new HashMap<String, FileSplitterEnum>();

    static {
        for (FileSplitterEnum f : FileSplitterEnum.values()) {
            lookupMap.put(f.getDelimiter(), f);
            lookupMap.put(f.name().toLowerCase(), f);
            lookupMap.put(f.name().toUpperCase(), f);
        }
    }

    /**
     * Instantiates a new file data splitter.
     *
     * @param splitter the splitter
     */
    FileSplitterEnum(String splitter) {
        this.splitter = splitter;
    }

    /**
     * Gets the file data splitter.
     *
     * @param str the str
     * @return the file data splitter
     */
    public static FileSplitterEnum getFileDataSplitter(String str) {
        FileSplitterEnum f = lookupMap.get(str);
        if (f == null) {
            throw new IllegalArgumentException(
                    "[" + FileSplitterEnum.class + "] Unsupported FileDataSplitter:" + str);
        }
        return f;
    }

    /**
     * Gets the delimiter.
     *
     * @return the delimiter
     */
    public String getDelimiter() {
        return this.splitter;
    }
}
