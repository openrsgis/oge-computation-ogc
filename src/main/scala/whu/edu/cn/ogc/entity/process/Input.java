package whu.edu.cn.ogc.entity.process;

import java.util.List;

public class Input {
    private String title;
    private String description;
    /**
     * 表示这个Input在请求中至少出现几次
     */
    private Integer minOccurs;
    /**
     * 表示这个Input在请求中至多出现几次 如果json response 中是unbounded话，表示无上限 在此表示为null
     */
    private Integer maxOccurs;
    private List<String> metadata;
    private List<String> keywords;
    private Schema schema;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Integer getMinOccurs() {
        return minOccurs;
    }

    public void setMinOccurs(Integer minOccurs) {
        this.minOccurs = minOccurs;
    }

    public Integer getMaxOccurs() {
        return maxOccurs;
    }

    public void setMaxOccurs(Integer maxOccurs) {
        this.maxOccurs = maxOccurs;
    }

    public List<String> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<String> metadata) {
        this.metadata = metadata;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
