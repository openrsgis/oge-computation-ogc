package whu.edu.cn.ogc.entity.process;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.List;

/**
 * process description中描述input和output的schema，这里值覆盖了部分字段
 */
@Slf4j
public class Schema {
    /**
     * 数据类型 string object number array
     */
    private String type;
    /**
     * 指输出结果的编码方式 例如binary
     */
    private String contentEncoding;
    /**
     * 指输出结果的媒体类型，如application/tiff application=geotiff 指输出的数据类型
     */
    private String contentMediaType;
    /**
     * 描述具体的数据格式 double dateTime geojson-geometry ogc-bbox uri
     */
    private String format;
    /**
     * 枚举 表示只能从列出的枚举中选择数据
     */
    private List<Object> enumList;
    /**
     * 满足列出的schema列表中的一个即可
     */
    private List<Schema> oneOf;
    /**
     * 需要满足列出的所有schema
     */
    private List<Schema> allOf;
    /**
     * 在measureInput里面用来表示需要的哪些属性，例如 value uom
     */
    private List<String> required;
    /**
     * 在measureInput、Object中表示属性
     */
    private JSONObject properties;
    /**
     *  number（doubleInput）类型的最小值
     */
    private Double minimum;
    /**
     *  number（doubleInput）类型的最大值
     */
    private Double  maximum;
    /**
     * array 类型 最小的item数量
     */
    private Integer  minItems;
    /**
     * array 类型 最大的item数量
     */
    private Integer  maxItems;
    /**
     * array 类型 其item的schema
     */
    private Schema items;
    /**
     * 引用的其它JSON schema的关键字 例如 https://geojson.org/schema/FeatureCollection.json
     * 相当于引入了一个Schema，需要与之保持一致
     */
    private String $ref;

    public List<Object> getEnumList() {
        return enumList;
    }

    public void setEnumList(List<Object> enumList) {
        this.enumList = enumList;
    }

    public List<Schema> getAllOf() {
        return allOf;
    }

    public void setAllOf(List<Schema> allOf) {
        this.allOf = allOf;
    }

    public List<String> getRequired() {
        return required;
    }

    public void setRequired(List<String> required) {
        this.required = required;
    }

    public JSONObject getProperties() {
        return properties;
    }

    public void setProperties(JSONObject properties) {
        this.properties = properties;
    }

    public Double getMinimum() {
        return minimum;
    }

    public void setMinimum(Double minimum) {
        this.minimum = minimum;
    }

    public Double getMaximum() {
        return maximum;
    }

    public void setMaximum(Double maximum) {
        this.maximum = maximum;
    }

    public Integer getMinItems() {
        return minItems;
    }

    public void setMinItems(Integer minItems) {
        this.minItems = minItems;
    }

    public Integer getMaxItems() {
        return maxItems;
    }

    public void setMaxItems(Integer maxItems) {
        this.maxItems = maxItems;
    }

    public Schema getItems() {
        return items;
    }

    public void setItems(Schema items) {
        this.items = items;
    }

    public String get$ref() {
        return $ref;
    }

    public void set$ref(String $ref) {
        this.$ref = $ref;
    }

    public List<Schema> getOneOf() {
        return oneOf;
    }

    public void setOneOf(List<Schema> oneOf) {
        this.oneOf = oneOf;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public void setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
    }

    public String getContentMediaType() {
        return contentMediaType;
    }

    public void setContentMediaType(String contentMediaType) {
        this.contentMediaType = contentMediaType;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * 通过反射机制进行schema的合并
     * @param schema1 left schema1
     * @param schema2 right schema2
     * @return merged schema
     */
    public Schema combineSchemas(Schema schema1, Schema schema2){
        try{
            Schema newSchema = new Schema();
            // 获取Schema类的所有属性
            Field[] fields = Schema.class.getDeclaredFields();
            // 遍历所有属性
            for (Field field : fields) {
                // 设置属性可访问
                field.setAccessible(true);
                // 如果属性在第一个schema对象中有值，则将其赋值给newSchema对象
                if (field.get(schema1) != null) {
                    field.set(newSchema, field.get(schema1));
                }
                // 如果属性在第二个schema对象中有值，并且在第一个schema对象中没有值，则将其赋值给newSchema对象
                else if (field.get(schema2) != null) {
                    field.set(newSchema, field.get(schema2));
                }
            }
            return newSchema;
        } catch (Exception e){
            e.printStackTrace();
            log.error("合并schemas时出现错误");
            return null;
        }
    }
}
