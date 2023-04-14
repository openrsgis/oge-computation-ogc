package whu.edu.cn.ogc.entity.process;

public class QualifiedValue {
    private String mediaType;
    private String encoding;
    private Schema schema;
    private Object value;
    private String href;

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

    @Override
    public String toString() {
        return "QualifiedValue{" +
                "mediaType='" + mediaType + '\'' +
                ", encoding='" + encoding + '\'' +
                ", schema=" + schema +
                ", value=" + value +
                ", href='" + href + '\'' +
                '}';
    }
}
