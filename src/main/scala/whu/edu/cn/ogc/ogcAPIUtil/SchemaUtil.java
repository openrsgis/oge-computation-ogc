package whu.edu.cn.ogc.ogcAPIUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import whu.edu.cn.ogc.entity.coverage.Coverage;
import whu.edu.cn.ogc.entity.feature.Feature;
import whu.edu.cn.ogc.entity.featureCollection.FeatureCollection;
import whu.edu.cn.ogc.entity.process.*;
import whu.edu.cn.ogc.entity.process.Process;
import whu.edu.cn.ogc.entity.spatial.Extent;
import whu.edu.cn.ogc.entity.spatial.SpatialExtent;
import whu.edu.cn.ogc.entity.spatial.TemporalExtent;

import java.text.ParseException;
import java.util.*;

public class SchemaUtil {

    public Feature json2Feature(JSONObject featureObj){
        String id = featureObj.getString("id");
        JSONObject featureGeoJSon = new JSONObject();
        featureGeoJSon.put("geometry", featureObj.getJSONObject("geometry"));
        featureGeoJSon.put("properties", featureObj.getJSONObject("properties"));
        return new Feature(id, null, null, "Feature", featureGeoJSon);
    }

    public Coverage json2Coverage(JSONObject coverageObj){
        String id = coverageObj.getString("id");
        JSONObject domainSet = null;
        if(coverageObj.containsKey("domainset")){
            domainSet = coverageObj.getJSONObject("domainset");
        }
        JSONObject rangeType = null;
        if(coverageObj.containsKey("rangetype")){
            rangeType = coverageObj.getJSONObject("rangetype");
        }
        List<String> crs = new ArrayList<>();
        if(coverageObj.containsKey("crs")){
            crs = jsonArray2Crs(coverageObj.getJSONArray("crs"));
        }
        String bboxCrs = null;
        SpatialExtent spatialExtent = null;
        if(coverageObj.containsKey("extent")){
            if(coverageObj.getJSONObject("extent").containsKey("spatial")){
                SpatialUtil spatialUtil = new SpatialUtil();
                List<List<Float>> bbox = jsonArray2Bbox(coverageObj.getJSONObject("extent").getJSONObject("spatial").getJSONArray("bbox"));
                bboxCrs = spatialUtil.crsUri2Crs(coverageObj.getJSONObject("extent").getJSONObject("spatial").getString("crs"));
                spatialExtent = new SpatialExtent(bbox, bboxCrs);
            }
        }
        JSONArray linksArr = coverageObj.getJSONArray("links");
        List<Link> coverageLinks = new ArrayList<>();
        for(int index = 0; index < linksArr.size(); index++){
            JSONObject linkObj = linksArr.getJSONObject(index);
            if(linkObj.getString("rel").equals("http://www.opengis.net/def/rel/ogc/1.0/coverage")){
                coverageLinks.add(json2Link(linkObj));
            }
        }
        return new Coverage(id, crs, null, "Coverage", spatialExtent, domainSet, rangeType, coverageLinks);
    }

    /**
     * 从featureCollectionObj中抽取出元数据和实际数据，元数据就是meta，实际数据就是value
     * @param featureCollectionObj {meta：, value: }
     * @return FeatureCollection
     */
    public FeatureCollection json2FeatureCollection(JSONObject featureCollectionObj){
        JSONObject metaObj = featureCollectionObj.getJSONObject("meta");
        String productId = metaObj.getString("id");
        Extent extent = json2Extent(metaObj);
        String crs = null;
        if(metaObj.containsKey("crs")){
            SpatialUtil spatialUtil = new SpatialUtil();
            crs = spatialUtil.crsUri2Crs(metaObj.getString("crs"));
        }
        String description = metaObj.getString("description");
        String itemType = "feature";
        JSONObject valueObj = featureCollectionObj.getJSONObject("value");
        JSONArray features = valueObj.getJSONArray("features");
        int size = features.size();
        return new FeatureCollection(productId, size, description, crs, itemType, extent, features);
    }

    /**
     *
     * @param collectionArray
     * @param baseUrl
     * @param productId
     * @return
     * @throws FactoryException
     * @throws TransformException
     * @throws ParseException
     */
//    public CoverageCollection json2CoverageCollection(JSONArray collectionArray, String baseUrl, String productId) throws FactoryException, TransformException, ParseException {
//        String crs = null;
//        List<Coverage> coverageList = new ArrayList<>();
//        Extent extent = new Extent();
//        for(int index = 0; index < collectionArray.size(); index++){
//            JSONObject collectionObj = collectionArray.getJSONObject(index);
//            extent = json2Extent(collectionObj);
//            if(collectionObj.containsKey("crs")){
//                crs = spatialUtil.crsUri2Crs(collectionObj.getString("crs"));
//            }
//            coverageList.add(ogcAPI.getCoverage(baseUrl, collectionObj.getString("id")));
//        }
//        String itemType = "coverage";
//        return new CoverageCollection(productId, collectionArray.size(), null, crs, itemType, extent, coverageList);
//    }

    /**
     *
     * @param collectionObj the jsonObject of the collection
     * @return Extent
     */
    public Extent json2Extent(JSONObject collectionObj) {
        Extent extent = new Extent();
        List<List<Float>> bbox;
        String bboxCrs;
        List<List<String>> interval;
        if(collectionObj.containsKey("extent")){
            if(collectionObj.getJSONObject("extent").containsKey("spatial")){
                SpatialUtil spatialUtil = new SpatialUtil();
                bbox = jsonArray2Bbox(collectionObj.getJSONObject("extent").getJSONObject("spatial").getJSONArray("bbox"));
                bboxCrs = spatialUtil.crsUri2Crs(collectionObj.getJSONObject("extent").getJSONObject("spatial").getString("crs"));
                extent.setSpatial(new SpatialExtent(bbox, bboxCrs));
            }
            if(collectionObj.getJSONObject("extent").containsKey("temporal")){
                JSONArray intervalList = collectionObj.getJSONObject("extent").getJSONObject("temporal").getJSONArray("interval");
                interval = jsonArray2Interval(intervalList);
                extent.setTemporal(new TemporalExtent(interval));
            }
        }
        return extent;
    }


    /**
     * json编码的Link信息转Link类对象 描述链接的资源
     * @param linkObj json格式的Link
     * @return Schema对象
     */
    public Link json2Link(JSONObject linkObj){
        Link link = new Link();
        if(linkObj.containsKey("type")){
            link.setType(linkObj.getString("type"));
        }
        if(linkObj.containsKey("rel")){
            link.setRel(linkObj.getString("rel"));
        }
        if(linkObj.containsKey("title")){
            link.setTitle(linkObj.getString("title"));
        }
        if(linkObj.containsKey("href")){
            link.setHref(linkObj.getString("href"));
        }
        if(linkObj.containsKey("hreflang")){
            link.setHreflang(linkObj.getString("hreflang"));
        }
        return link;
    }


    /**
     * json编码转Schema 类对象 Schema类用于存储Input/Output里面的数据输入/输出要求
     * @param schemaObj json格式的schema
     * @return Schema对象
     */
    public Schema json2Schema(JSONObject schemaObj){
        Schema schema = new Schema();
        if(schemaObj.containsKey("type")){
            schema.setType(schemaObj.getString("type"));
        }
        if(schemaObj.containsKey("format")){
            schema.setFormat(schemaObj.getString("format"));
        }
        if(schemaObj.containsKey("contentMediaType")){
            schema.setContentMediaType(schemaObj.getString("contentMediaType"));
        }
        if(schemaObj.containsKey("contentEncoding")){
            schema.setContentEncoding(schemaObj.getString("contentEncoding"));
        }
        if(schemaObj.containsKey("oneOf")){
            List<Schema> schemaList = new ArrayList<>();
            JSONArray schemaArray = schemaObj.getJSONArray("oneOf");
            for(int i = 0; i < schemaArray.size(); i++){
               JSONObject childSchemaObj = schemaArray.getJSONObject(i);
               schemaList.add(json2Schema(childSchemaObj));
            }
            schema.setOneOf(schemaList);
        }
        if(schemaObj.containsKey("allOf")){
            List<Schema> schemaList = new ArrayList<>();
            JSONArray schemaArray = schemaObj.getJSONArray("allOf");
            for(int i = 0; i < schemaArray.size(); i++){
                JSONObject childSchemaObj = schemaArray.getJSONObject(i);
                schemaList.add(json2Schema(childSchemaObj));
            }
            schema.setAllOf(schemaList);
        }
        if(schemaObj.containsKey("enum")){
            schema.setEnumList(schemaObj.getJSONArray("enum").toJavaList(Object.class));
        }
        if(schemaObj.containsKey("required")){
            schema.setRequired(schemaObj.getJSONArray("required").toJavaList(String.class));
        }
        if(schemaObj.containsKey("properties")){
            schema.setProperties(schemaObj.getJSONObject("properties"));
        }
        if(schemaObj.containsKey("minimum")){
            schema.setMinimum(schemaObj.getDouble("minimum"));
        }
        if(schemaObj.containsKey("maximum")){
            schema.setMaximum(schemaObj.getDouble("maximum"));
        }
        if(schemaObj.containsKey("minItems")){
            schema.setMinItems(schemaObj.getInteger("minItems"));
        }
        if(schemaObj.containsKey("maxItems")){
            schema.setMaxItems(schemaObj.getInteger("maxItems"));
        }
        if(schemaObj.containsKey("items")){
            schema.setItems(json2Schema(schemaObj.getJSONObject("items")));
        }
        if(schemaObj.containsKey("$ref")){
            schema.set$ref(schemaObj.getString("$ref"));
        }
        return schema;
    }


    /**
     * input描述信息的json编码转Input 类对象
     * @param inputObj json格式的input
     * @return Input对象
     */
    public Input json2Input(JSONObject inputObj){
        Input input = new Input();
        if(inputObj.containsKey("title")){
            input.setTitle(inputObj.getString("title"));
        }
        if(inputObj.containsKey("schema")){
            input.setSchema(json2Schema(inputObj.getJSONObject("schema")));
        }
        if(inputObj.containsKey("description")){
            input.setDescription(inputObj.getString("description"));
        }
        if(inputObj.containsKey("minOccurs")){

            input.setMinOccurs(inputObj.getInteger("minOccurs"));
        }
        if(inputObj.containsKey("maxOccurs")){
            Object maxOccurs = inputObj.get("maxOccurs");
            if(maxOccurs instanceof Integer){
                input.setMaxOccurs(inputObj.getInteger("maxOccurs"));
            }
        }
        if(inputObj.containsKey("keywords") && (inputObj.getJSONArray("keywords")!=null)){
            input.setKeywords(inputObj.getJSONArray("keywords").toJavaList(String.class));
        }
        if(inputObj.containsKey("metadata") && (inputObj.getJSONArray("metadata")!=null)){
            input.setMetadata(inputObj.getJSONArray("metadata").toJavaList(String.class));
        }
        return input;
    }


    /**
     * output描述信息的json编码转Output 类对象
     * @param outputObj json格式的output
     * @return Output对象
     */
    public Output json2Output(JSONObject outputObj){
        Output output = new Output();
        if(outputObj.containsKey("title")){
            output.setTitle(outputObj.getString("title"));
        }
        if(outputObj.containsKey("description")){
            output.setDescription(outputObj.getString("description"));
        }
        if(outputObj.containsKey("schema")){
            output.setSchema(json2Schema(outputObj.getJSONObject("schema")));
        }
        if(outputObj.containsKey("metadata") && (outputObj.getJSONArray("metadata")!=null)){
            output.setMetadata(outputObj.getJSONArray("metadata").toJavaList(String.class));
        }
        if(outputObj.containsKey("keywords") && (outputObj.getJSONArray("keywords")!=null)){
            output.setKeywords(outputObj.getJSONArray("keywords").toJavaList(String.class));
        }
        return output;
    }

    /**
     * json编码的Process 描述信息转换为Process对象
     * @param processObj json格式的Process描述信息
     * @return Process对象
     */
    public Process json2Process(JSONObject processObj){
        Process process = new Process();
        if(processObj.containsKey("version")){
            process.setVersion(processObj.getString("version"));
        }
        if(processObj.containsKey("id")){
            process.setId(processObj.getString("id"));
        }
        if(processObj.containsKey("title")){
            process.setTitle(processObj.getString("title"));
        }
        if(processObj.containsKey("description")){
            process.setDescription(processObj.getString("description"));
        }
        if(processObj.containsKey("keywords") && (processObj.getJSONArray("keywords")!=null)){
            process.setKeywords(processObj.getJSONArray("keywords").toJavaList(String.class));
        }
        if(processObj.containsKey("jobControlOptions") && (processObj.getJSONArray("jobControlOptions")!=null)){
            process.setJobControlOptions(processObj.getJSONArray("jobControlOptions").toJavaList(String.class));
        }
        if(processObj.containsKey("outputTransmission") && (processObj.getJSONArray("outputTransmission")!=null)){
            process.setOutputTransmission(processObj.getJSONArray("outputTransmission").toJavaList(String.class));
        }
        if(processObj.containsKey("links")){
            JSONArray linkArray = processObj.getJSONArray("links");
            List<Link> linkList = new ArrayList<>();
            for(int index = 0; index < linkArray.size(); index++){
                linkList.add(json2Link(linkArray.getJSONObject(index)));
            }
            process.setLinks(linkList);
        }
        if(processObj.containsKey("inputs")){
            Map<String, Input> inputMap = new HashMap<>();
            JSONObject inputsObj = processObj.getJSONObject("inputs");
            for (Map.Entry<String, Object> entry : inputsObj.entrySet()) {
                String key  = entry.getKey();
                Input input = json2Input((JSONObject) entry.getValue());
                inputMap.put(key, input);
            }
            process.setInputs(inputMap);
        }
        if(processObj.containsKey("outputs")){
            Map<String, Output> outputMap = new HashMap<>();
            JSONObject outputObj = processObj.getJSONObject("outputs");
            for (Map.Entry<String, Object> entry : outputObj.entrySet()) {
                String key  = entry.getKey();
                Output output = json2Output((JSONObject) entry.getValue());
                outputMap.put(key, output);
            }
            process.setOutputs(outputMap);
        }
        return process;
    }



    /**
     * the jsonArray in response from the OGC API, and transform to the List<List<Float>>
     * @param bboxArray the jsonArray format response of bbox
     * @return List<List<Float>>
     */
    public List<List<Float>> jsonArray2Bbox(JSONArray bboxArray){
        List<List<Float>> bboxLists = new ArrayList<>();
        for(int index = 0; index < bboxArray.size(); index++){
            bboxLists.add(bboxArray.getJSONArray(index).toJavaList(Float.class));
        }
        return bboxLists;
    }


    /**
     * the interval jsonArray in response from the OGC API, and transform to the List<List<String>>
     * @param intervalArray the jsonArray format response of interval
     * @return List<List<String>>
     */
    public List<List<String>> jsonArray2Interval(JSONArray intervalArray){
        List<List<String>> intervalList = new ArrayList<>();
        for(int index = 0; index < intervalArray.size(); index++){
            intervalList.add(intervalArray.getJSONArray(index).toJavaList(String.class));
        }
        return intervalList;
    }


    /**
     * 将crs:["http://www.opengis.net/def/crs/OGC/1.3/CRS84"] 转换成 ["EPSG:4326"]
     * @param crsArray crs array, for example  "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
     * @return EPSG的标识符
     */
    public List<String> jsonArray2Crs(JSONArray crsArray){
        List<String> EPSGCrsList = new ArrayList<>();
        for(int index = 0 ; index < crsArray.size(); index ++ ){
            String crs = crsArray.getString(index);
            SpatialUtil spatialUtil = new SpatialUtil();
            EPSGCrsList.add(spatialUtil.crsUri2Crs(crs));
        }
        return EPSGCrsList;
    }


    /**
     * 根据Process的描述，组装Process的请求体
     * @param process 请求到的Process描述
     * @param inputParams 输入参数，是一个字典
     * @return 组装好的请求Process execution的RequestBody
     */
    public ProcessRequestBody assemblyRequestBody(Process process, Map<String, String> inputParams){
        ProcessRequestBody processRequestBody = new ProcessRequestBody();
        JSONObject inputsObj = new JSONObject();
        Map<String, Input> processInputs = process.getInputs();
        processInputs.forEach((key, input)->{
            // 如果输入的参数里也有这个参数，进行格式转换，因为目前输入的都是字符串
            if(inputParams.containsKey(key)){
                inputsObj.put(key, transformationParam(key, input, inputParams.get(key)).get("value"));
            }
        });
        processRequestBody.setInputs(inputsObj);
        return processRequestBody;
    }

    /**
     * 根据输入的参数和对应的schema做必要的转换
     * @param inputName input的Name
     * @param inputMeta process description中的描述信息
     * @param inputParam DAG输入的参数
     * @return
     */
    public JSONObject transformationParam(String inputName, Input inputMeta, String inputParam){
        Schema schema = inputMeta.getSchema();
        // 将schema转换为json形式 其中value是null时key将不记录
        JSONObject inputParamObj = new JSONObject();
        if(schema.getAllOf() != null){
            List<Schema> allOfSchemaList = schema.getAllOf();
            Schema combinedSchema = new Schema();
            //合并各个schema
            for(Schema oneOfSchema : allOfSchemaList){
                combinedSchema = combinedSchema.combineSchemas(combinedSchema, oneOfSchema);
            }
            inputMeta.setSchema(combinedSchema);
            return transformationParam(inputName, inputMeta, inputParam);
        }else if(schema.getOneOf() != null){
            // 默认就是第一个 用第一个contentMediaType 和 contentEncoding
            List<Schema> schemaList = schema.getOneOf();
            // 问题就是如何判断、采用哪一个schema，现在采用的策略是将schemaList schema一一灌输进行，若返回的不是null，就直接使用
            for(Schema schemaObj: schemaList){
                inputParamObj = transformationParamWithOneSchema(inputName, inputMeta, inputParam, true);
                if(inputParamObj != null){
                    break;
                }
            }
        }else{
            // 既没有allOf也没有oneOf 只有一个schema
            inputParamObj = transformationParamWithOneSchema(inputName, inputMeta, inputParam, false);

        }
        return inputParamObj;
    }

    /**
     * 根据单个schema校验和转换输入参数
     * @param inputName 输入参数名称
     * @param inputMeta 输入参数元数据
     * @param inputParam 具体输入的参数
     * @return
     */
    public JSONObject transformationParamWithOneSchema(String inputName, Input inputMeta, String inputParam, boolean ifOneOf){
        Schema schema = inputMeta.getSchema();
        // 将schema转换为json形式 其中value是null时key将不记录
        JSONObject inputParamObj = new JSONObject();
        // 如果不是oneOf, 就不需要检查contentMediaType
        if(!ifOneOf){
            if(schema.getType() != null){
                switch (schema.getType()) {
                    case "string":
                        // 普通字符串 or Byte
                        if (schema.getFormat() == null || schema.getFormat().equals("byte")) {
                            inputParamObj.put(inputName, inputParam);
                        } else if (schema.getFormat().equals("dateTime")) {
                            //TODO 检验时间格式并做必要的转换
                            inputParamObj.put(inputName, inputParam);
                        }
                        break;
                    case "number":
                        if (schema.getType() == null || schema.getFormat().equals("double")) {
                            //如果没说是什么format 默认用double
                            inputParamObj.put(inputName, Double.parseDouble(inputParam));
                        } else if (schema.getFormat().equals("float")) {
                            inputParamObj.put(inputName, Float.parseFloat(inputParam));
                        }
                        break;
                    case "integer":
                        inputParamObj.put(inputName, Integer.parseInt(inputParam));
                        break;
                    case "boolean":
                        // 是"true"则返回true，否则返回false
                        inputParamObj.put(inputName, inputParam.equalsIgnoreCase("true"));
                        break;
                    case "array":
                        // 直接转换为jsonArray 不检验items了
                        //TODO 检验jsonArray的items
                        inputParamObj.put(inputName, JSONArray.parse(inputParam));
                        break;
                    case "object":
                        QualifiedValue qualifiedValue = string2QualifiedValue(inputMeta, inputParam);
                        inputParamObj.put(inputName, JSON.parseObject(JSON.toJSONString(qualifiedValue)));
                        break;
                }
            }
        }else{ // 都是返回的QualifiedValue
            String contentEncoding = schema.getContentEncoding();
            String contentMediaType = schema.getContentMediaType();
            QualifiedValue qualifiedValue = new QualifiedValue();
            // 是oneOf 需要检查contentMedia 等,这里就写几个常用的，无法面面俱到
            if(schema.getFormat() != null){
                switch (schema.getFormat()){
                    case "ogc-bbox":
                        // 一般输入一个bbox数组，需要包装成bbox的格式
                        JSONObject bboxObj = new JSONObject();
                        bboxObj.put("bbox", JSONArray.parse(inputParam));
                        bboxObj.put("crs", "http://www.opengis.net/def/crs/OGC/1.3/CRS84");
                        inputParamObj.put(inputName, bboxObj);
                        break;
                    case "geojson-feature-collection":
                        // TODO OGE 的 Feature Collection 如何转换为geojson
                        break;
                    case "geojson-feature":
                        // TODO OGE 的 Feature 如何转换为geojson
                        break;
                    case "geojson-geometry":
                        // TODO OGE 的 geometry 如何转换为geojson
                        break;
                }
            }
            if(schema.getType() != null){
                if(schema.getType().equals("string")){
                    // 对于binary类型
                    if(contentEncoding.equals("binary")){
                        // 不检验字节码是否符合要求了 直接拼装
                        qualifiedValue.setValue(inputParam);
                        qualifiedValue.setMediaType(contentMediaType);
                    }else if(contentMediaType.contains("application/gml+xml")){
                        // gml 字符串
                        // TODO 可以再次检查是否是gml
                        qualifiedValue.setValue(inputParam);
                        qualifiedValue.setMediaType(contentMediaType);
                    }else if(contentMediaType.contains("application/tiff")|| contentMediaType.contains("application/geotiff")){
                        // 生产影像的url
                        //TODO 这里有个问题 是用href来表达影像还是用value
                        qualifiedValue.setValue(inputParam);
                        qualifiedValue.setMediaType(contentMediaType);
                    }
                }
            }
        }

        return inputParamObj;
    }

//    public boolean isSchemaByValue(Schema schema, String inputParam){
//        boolean flag = false;
//        String contentEncoding = schema.getContentEncoding();
//        String contentMediaType = schema.getContentMediaType();
//        // 如果都是null 则直接返回true
//        if(contentEncoding == null && contentMediaType == null){
//            String $ref = schema.get$ref();
//            if($ref == null){
//                // 没有什么要求 直接返回true
//                return true;
//            }
//            // TODO 按理说应该请求ref获取schema 这里直接硬性判断
//            if($ref.equals("http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/geometryGeoJSON.json")){
//                return formatUtil.isGeoJSON(inputParam);
//            }
//        }else{
//            if(contentEncoding.equals("binary")){
//
//            }
//        }
//        return flag;
//    }
    /**
     * string转换为QualifiedValue，适用于Object的输入
     * @param inputMeta input描述信息
     * @param inputParam 输入的String类型的输入参数
     * @return QualifiedValue
     */
    public QualifiedValue string2QualifiedValue(Input inputMeta, String inputParam){
        Schema schema = inputMeta.getSchema();
        QualifiedValue qualifiedValue = new QualifiedValue();
        if(inputMeta.getMinOccurs() == null || inputMeta.getMinOccurs() == 1 ||
                (inputMeta.getMaxOccurs() != null && inputMeta.getMinOccurs() == 1)){
            qualifiedValue.setValue(JSON.parseObject(inputParam));
        }
        if(schema.getContentMediaType() !=null ){
            qualifiedValue.setMediaType(schema.getContentMediaType());
        }
        return qualifiedValue;
    }

    /**
     * 验证输入参数的类型
     * @param schemaObj
     * @param inputParam
     * @return
     */
    public String param2String(JSONObject schemaObj, Object inputParam){
        Set<String> keys = schemaObj.keySet();
        if(keys.size() == 1){
            return (String)inputParam;
        }else if(schemaObj.containsKey("contentEncoding") && schemaObj.containsKey("contentMediaType")){
            // 这里可以加个函数，返回相应的字符串
            return "";
        }
        return "";
    }

    public  static void main(String [] args){
        SchemaUtil schemaUtil = new SchemaUtil();
//        System.out.println(schemaUtil.getDateFormat("2000-10-30T18:24:39+00:00"));
        JSONObject obj = new JSONObject();
        obj.put("name", null);
        obj.put("a", "as");
        String a = obj.getString("name");
        String b = "1";
//        System.out.println(schemaUtil.getDateFormat("2000-10-30 18:24:39"));
    }
}
