package whu.edu.cn.ogc.ogcAPIUtil;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.springframework.http.HttpMethod;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import whu.edu.cn.application.oge.HttpRequest;
import whu.edu.cn.ogc.entity.coverage.Coverage;
import whu.edu.cn.ogc.entity.coverageCollection.CoverageCollection;
import whu.edu.cn.ogc.entity.feature.Feature;
import whu.edu.cn.ogc.entity.featureCollection.FeatureCollection;
import whu.edu.cn.ogc.entity.process.JobResponse;
import whu.edu.cn.ogc.entity.process.JobStatus;
import whu.edu.cn.ogc.entity.process.ProcessRequestBody;
import whu.edu.cn.ogc.entity.processes.Processes;
import whu.edu.cn.ogc.entity.process.Process;
import whu.edu.cn.ogc.entity.spatial.Extent;
import whu.edu.cn.ogc.entity.spatial.SpatialExtent;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class OgcAPI {

    public JSONObject landingPage(String baseUrl){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(baseUrl, params, HttpMethod.GET);
        return JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
    }

    /**
     * 查询/collections返回的结果集，将符合条件的资源元数据返回，包括featureCollection Coverage Map 等
     * @param baseUrl 基本url
     * @param productId 产品Id
     * @param collectionId 集合Id
     * @param bbox 空间范围筛选条件
     * @param bboxCrs 空间范围坐标系
     * @param dateTime 时间
     * @param type 类型 Feature Coverage
     * @return 返回一个JSONArray
     */
    public JSONArray queryCollection(String baseUrl, String productId, String collectionId,  List<Float> bbox, String bboxCrs, List<String> dateTime, String type) {
        try{
            MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
            String resultStr = APIHttpUtil.formHttp(baseUrl + "collections", params, HttpMethod.GET);
            JSONObject responseObj = JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
            JSONArray collectionArray = responseObj.getJSONArray("collections");
            JSONArray resultCollection = new JSONArray();
            for(int index = 0; index < collectionArray.size(); index++){
                boolean spatialFilter = false;
                boolean timeFilter = false;
                boolean productFilter = false;
                boolean typeFilter = false;
                boolean collectionIdFilter = false;
                if(bbox == null){
                    spatialFilter = true;
                }
                if(dateTime == null){
                    timeFilter = true;
                }
                if(productId == null){
                    productFilter = true;
                }
                if(type == null){
                    typeFilter = true;
                }
                if(collectionId == null){
                    collectionIdFilter = true;
                }
                JSONObject collectionObj = collectionArray.getJSONObject(index);
                if(type !=null){
                    if(type.equals("coverage")){
                        JSONArray linkArray = collectionObj.getJSONArray("links");
                        for(int linkIndex = 0; linkIndex<linkArray.size(); linkIndex++){
                            // 获取Coverage 一定是这个
                            if(linkArray.getJSONObject(linkIndex).getString("rel").equals("http://www.opengis.net/def/rel/ogc/1.0/coverage")){
                                typeFilter = true;
                            }
                        }
                    }else if(type.equals("feature")){
                        if(collectionObj.containsKey("itemType") && collectionObj.getString("itemType").equals("feature")){
                            typeFilter = true;
                        }
                        JSONArray linkArray = collectionObj.getJSONArray("links");
                        for(int linkIndex = 0; linkIndex<linkArray.size(); linkIndex++){
                            // 获取Coverage 一定是这个
                            if(linkArray.getJSONObject(linkIndex).getString("rel").equals("items")){
                                typeFilter = true;
                            }
                        }
                    }
                }
                JSONObject extent = collectionObj.getJSONObject("extent");
                if(extent.containsKey("spatial") && bbox != null){
                    JSONArray collectionBboxArray = extent.getJSONObject("spatial").getJSONArray("bbox");
                    String collectionBboxCrs = extent.getJSONObject("spatial").getString("crs");
                    for(int index2 = 0; index2 < collectionBboxArray.size(); index2++){
                        List<Float> collectionBbox = collectionBboxArray.getJSONArray(index2).toJavaList(Float.class);
                        if(SpatialUtil.isHavePublicArea(bbox, collectionBbox, bboxCrs, collectionBboxCrs)){
                            spatialFilter = true;
                            break;
                        }
                    }
                }
                if(extent.containsKey("temporal") && dateTime != null){
                    JSONArray collectionIntervalArray =  extent.getJSONObject("temporal").getJSONArray("interval");
                    for(int index3 = 0; index3 < collectionIntervalArray.size(); index3++){
                        List<String> collectionInterval = collectionIntervalArray.getJSONArray(index3).toJavaList(String.class);
                        DataTimeUtil dataTimeUtil = new DataTimeUtil();
                        if(dataTimeUtil.isHavePublicTime(dateTime, collectionInterval)){
                            timeFilter = true;
                            break;
                        }
                    }
                }
                if(collectionObj.containsKey("keywords") && productId !=null){
                    List<String> keywordsList = collectionObj.getJSONArray("keywords").toJavaList(String.class);
                    if(keywordsList.contains(productId)){
                        productFilter = true;
                    }
                }
                if(collectionId != null){
                    if(collectionObj.getString("id").equals(collectionId)){
                        collectionIdFilter = true;
                    }
                }
                if(spatialFilter && timeFilter && productFilter && typeFilter && collectionIdFilter){
                    resultCollection.add(collectionObj);
                }
            }
            return resultCollection;
        }catch (Exception e){
            e.printStackTrace();
            log.info("查询/collections出现异常");
            return null;
        }
    }

    public JSONArray getCollections(String baseUrl, List<Float> bbox, String bboxCrs, List<String> dateTime) throws FactoryException, TransformException, ParseException {
        if(bboxCrs == null){
            bboxCrs = "EPSG:4326";
        }
        return queryCollection(baseUrl, null, null,bbox, bboxCrs, dateTime, null);
    }

    public JSONObject getCollection(String baseUrl, String collectionId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(baseUrl + "collections/" + collectionId, params, HttpMethod.GET);
        return JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
    }

    public FeatureCollection getFeatureCollection(String baseUrl, String collectionId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        JSONObject metaDataObj = getCollection(baseUrl, collectionId);
        String resultStr = APIHttpUtil.formHttp(baseUrl + "collections/" + collectionId + "/items", params, HttpMethod.GET);
        JSONObject featureCollectionObj = JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
        JSONObject responseObj = new JSONObject();
        responseObj.put("meta", metaDataObj);
        responseObj.put("value", featureCollectionObj);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.json2FeatureCollection(responseObj);
    }

    public CoverageCollection getCoverageCollection(String baseUrl, String productId, List<Float> bbox, String bboxCrs, List<String> dateTime) throws FactoryException, TransformException, ParseException {
        if(bboxCrs == null){
            bboxCrs = "EPSG:4326";
        }
        JSONArray coverageCollectionArr = queryCollection(baseUrl, productId, null, bbox, bboxCrs, dateTime, "coverage");
        List<Coverage> coverageList = new ArrayList<>();
        List<Float> bboxList = new ArrayList<>();
        String firstCrs = null;
        for(int index = 0; index < coverageCollectionArr.size(); index++){
            JSONObject collectionObj = coverageCollectionArr.getJSONObject(index);
            Coverage coverage= getCoverage(baseUrl, collectionObj.getString("id"));
            SpatialExtent spatialExtent = coverage.getSpatialExtent();
            if(spatialExtent.getBbox()!=null && spatialExtent.getBbox().size() == 1){
                SpatialUtil spatialUtil = new SpatialUtil();
                bboxList = spatialUtil.mergeBbox(bboxList, spatialExtent.getBbox().get(0), firstCrs, spatialExtent.getCrs());
            }
            coverageList.add(getCoverage(baseUrl, collectionObj.getString("id")));
        }
        Extent extent = new Extent();
        List<List<Float>> mergeBbox = new ArrayList<>();
        mergeBbox.add(bboxList);
        extent.setSpatial(new SpatialExtent(mergeBbox, "EPSG:4326"));
        return new CoverageCollection(productId, coverageList.size(), null, null, "coverage", extent, coverageList);
    }

    public Feature getFeature(String baseUrl, String collectionId, String featureId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        SchemaUtil schemaUtil = new SchemaUtil();
        String resultStr = APIHttpUtil.formHttp(baseUrl + "collections/" + collectionId + "/items/" + featureId, params, HttpMethod.GET);
        return schemaUtil.json2Feature(JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect));
    }

    public Coverage getCoverage(String baseUrl, String coverageId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(baseUrl + "collections/" + coverageId, params, HttpMethod.GET);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.json2Coverage(JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect), baseUrl);
//        return schemaUtil.json2Coverage(queryCollection(baseUrl, productId, coverageId, null, null, null, null));
    }

    public JSONObject getCoverageValue(String baseUrl, String collectionId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(baseUrl + "collections/" + collectionId + "/coverage", params, HttpMethod.GET);
        return JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
    }

    public Processes getProcesses(String baseUrl){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String processesStr = APIHttpUtil.formHttp(baseUrl + "processes", params,HttpMethod.GET);
        JSONObject processesObj = JSONObject.parseObject(processesStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
        JSONArray processArray = processesObj.getJSONArray("processes");
        Processes processes = new Processes();
        List<Process> processList = new ArrayList<>();
        for(int index = 0; index < processArray.size(); index++){
            String processId = processArray.getJSONObject(index).getString("id");
            Process process = getProcess(baseUrl, processId);
            processList.add(process);
        }
        processes.setProcesses(processList);
        return processes;
    }

    public Process getProcess(String baseUrl, String processId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(baseUrl + "processes/" + processId, params, HttpMethod.GET);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.json2Process(JSONObject.parseObject(resultStr,com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect));
    }

    public Process getProcess(String processUrl){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(processUrl, params, HttpMethod.GET);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.json2Process(JSON.parseObject(resultStr,com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect));
    }

    /**
     *
     * @param params 代码编辑器生成的json格式的输入 示例：
     *               {"message": "hello",
     * 				  "name": "wkx",
     * 				  "url": "http//127.0.0.1:5000/process/hello-world"}
     * 				 经过Scala脚本的转换后为key value的形式 value一定是String格式
     * @return 待定
     */
    public JSONObject executeProcess(Map<String, String> params){
        // url example：http//localhost:8080/process/Slope
        String processUrl = (String) params.get("url");
        Process process = getProcess(processUrl);
        SchemaUtil schemaUtil = new SchemaUtil();
        // 解析Process的描述信息，组装结构体
        ProcessRequestBody processRequestBody = schemaUtil.assemblyRequestBody(process, params);
        List<String> jobControlOptions = process.getJobControlOptions();
        if(jobControlOptions.contains("async-execute")){
            // 异步执行 优先
            return asyncExecuteProcess(processRequestBody, processUrl);
        }else if(jobControlOptions.contains("sync-execute")){
            // 同步执行
           return JSONObject.parseObject(APIHttpUtil.jsonHttp(processUrl + "/execution", JSON.toJSONString(processRequestBody), HttpMethod.POST),com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
        }else{
            log.info("没有正确的执行模式");
            return null;
        }
    }
    /**
     * 根据jobId 获取Job
     * @param url 包含processId的url，例如http//127.0.0.1:5000/process/hello-world
     * @param jobId job Id
     * @return JobResponse Job的响应
     */
    public JobResponse getJobByJobId(String url, String jobId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        url = url + "/" + jobId;
        String resultStr = APIHttpUtil.formHttp(url, params, HttpMethod.GET);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.json2JobResponse(JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect));
    }

    /**
     * 根据jobId 获取处理结果
     * @param processUrl process的基本url
     * @param jobId jobId
     * @return JSONObject
     */
    public JSONObject getJobResult(String processUrl, String jobId){
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        String resultStr = APIHttpUtil.formHttp(processUrl + "/jobs/" + jobId + "/results", params, HttpMethod.GET);
        return JSONObject.parseObject(resultStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect);
    }

    /**
     * 异步执行
     * @param processRequestBody process执行请求体
     * @param processUrl process基本url
     * @return JSONObject
     */
    public JSONObject asyncExecuteProcess(ProcessRequestBody processRequestBody, String processUrl){
        SchemaUtil schemaUtil = new SchemaUtil();
        String jobId = "";
        String jobResponseStr =  APIHttpUtil.jsonHttp(processUrl + "/execution", processRequestBody.toString(), HttpMethod.POST);
        JobResponse jobResponse = schemaUtil.json2JobResponse(JSONObject.parseObject(jobResponseStr, com.alibaba.fastjson.parser.Feature.DisableSpecialKeyDetect));
        if (jobResponse.getJobID()!=null) {
            jobId = jobResponse.getJobID();
        }else{
            log.info("初始执行失败");
            return null;
        }
        // 休息500毫秒
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true){
            jobResponse = getJobByJobId(processUrl, jobId);
            String status = jobResponse.getStatus();
            if(status.equals(JobStatus.SUCCESSFUL.getStatus())){
                return getJobResult(processUrl, jobId);
            }else if(status.equals(JobStatus.FAILED.getStatus())){
                log.info(jobId + "执行状态为failed");
                return null;
            }else if(status.equals(JobStatus.DISMISSED.getStatus())){
                log.info(jobId + "已经取消");
            }else if(status.equals(JobStatus.ACCEPTED.getStatus()) || status.equals(JobStatus.RUNNING.getStatus())){
                // 休息500毫秒
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                // 其它情况 直接返回 null
                return null;
            }
        }
    }


    /**
     * 查看特定coverageInputName所支持的媒体类型
     * @param params 输入的参数
     * @param coverageInputName coverageInputName
     * @return List<String>
     */
    public List<String> getTypeOfCoverageInput(Map<String, String> params, String coverageInputName){
        String processUrl = (String) params.get("url");
        Process process = getProcess(processUrl);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.judgeCoverageBySchema(process.getInputs().get(coverageInputName).getSchema(), new ArrayList<String>());
    }

    /**
     * 查看特定coverageOutputName所支持的媒体类型
     * @param processUrl process API URL
     * @param coverageOutputName coverageOutputName
     * @return List<String> Coverage Type列表
     */
    public List<String> getTypeOfCoverageOutput(String processUrl, String coverageOutputName){
        Process process = getProcess(processUrl);
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.judgeCoverageBySchema(process.getOutputs().get(coverageOutputName).getSchema(), new ArrayList<>());
    }

    /**
     * 查看特定coverageOutputName所支持的媒体类型
     * @param process process
     * @param coverageOutputName coverageOutputName
     * @return List<String> Coverage Type列表
     */
    public List<String> getTypeOfCoverageOutput(Process process, String coverageOutputName){
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.judgeCoverageBySchema(process.getOutputs().get(coverageOutputName).getSchema(), new ArrayList<>());
    }

    /**
     * 查看特定featureOutputName所支持的媒体类型
     * @param process process
     * @param featureOutputName featureOutputName
     * @return List<String> Feature Type列表
     */
    public List<String> getTypeOfFeatureOutput(Process process, String featureOutputName){
        SchemaUtil schemaUtil = new SchemaUtil();
        return schemaUtil.judgeFeatureBySchema(process.getOutputs().get(featureOutputName).getSchema(), new ArrayList<>());
    }

    public static void main(String [] args) throws FactoryException, TransformException, ParseException, IOException {
        OgcAPI api = new OgcAPI();
//        JSONObject jsonObject = api.landingPage("http://10.100.66.24:5000/");
//        JSONObject jsonObject = api.getCollections("http://10.100.66.24:5000/");
//        JSONObject jsonObject = api.getCollection("http://10.100.66.24:5000/", "obs");
//        JSONObject jsonObject = api.getFeatureCollection("http://10.100.66.24:5000/", "obs");
//        JSONObject jsonObject = api.getFeature("http://10.100.66.24:5000/", "obs", "297");
//       Coverage coverage = api.getCoverage("http://127.0.0.1:5000/", "gdps-temperature");
//        LocalDateTime date = LocalDateTime.parse("2000-10-30T18:24:39+00:00", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
//______________________________________________________________________________________
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
//        Date date1 = sdf.parse("2000-10-30T18:24:39+00:00");
//        List<Float> geom = new ArrayList<Float>();
//        geom.add((float) 73.62);
//        geom.add((float) 18.19);
//        geom.add((float) 134.7601467382);
//        geom.add((float) 53.54);
//        List<String> times = new ArrayList<String>();
//        times.add("2001-10-30T18:24:39+00:00");
//        times.add("2002-10-30T18:24:39+00:00");
//        JSONArray jsonObject = api.getCollections("http://10.100.66.24:5000/", geom, null,times);
//______________________________________________________________________________________
        // CoverageCollection jsonObject = api.getCoverageCollection("http://10.100.66.24:5000/", "gdps",null, null,null);
//______________________________________________________________________________________
        // Process process = api.getProcess("http://10.100.66.24:5000/" , "hello-world");
//______________________________________________________________________________________
//        Processes processes = api.getProcesses("http://10.100.66.24:5000/");
//        Map<String, String> params = new HashMap<>();
//        params.put("url", "https://maps.gnosis.earth/ogcapi/processes/ElevationContours");
//        params.put("data", "https://maps.gnosis.earth/ogcapi/collections/SRTM_ViewFinderPanorama/coverage?subset=Lat(0:90),Lon(0:90)&scaleFactor=256&f=image/tiff");
//        params.put("distance", "1000");
//        params.put("minHeight", "-11000");
//        params.put("maxHeight", "9000");
//        params.put("geometryType", "lines");
//        JSONObject resultObj = api.executeProcess(params);
//        String a  = "";
// _____________________________________GET Coverage_________________________________________________
        Coverage coverage = api.getCoverage("https://maps.gnosis.earth/ogcapi/", "sentinel2-l2a");
        HttpRequest.writeTIFF(coverage.getCoverageLinks().get(1).getHref(), "E:\\LaoK\\data2\\test2.tif");
//        HttpRequest.writeTIFF();
        String a = "s";
    }
}
