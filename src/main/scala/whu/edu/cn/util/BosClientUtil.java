//package whu.edu.cn.util;
//
//import com.baidubce.auth.DefaultBceCredentials;
//import com.baidubce.services.bos.BosClient;
//import com.baidubce.services.bos.BosClientConfiguration;
//import com.baidubce.services.bos.model.GetObjectRequest;
//
//import java.io.File;
//
//public class BosClientUtil {
//    String ACCESS_KEY_ID = "ALTAKetCGvRVdSsIa1C9CR81Cm";
//    String SECRET_ACCESS_ID = "45624b0ae0c94c66877f75c6219b25f7";
//    String ENDPOINT = "https://s3.bj.bcebos.com";
//    public BosClient getClient() {
//        BosClientConfiguration config = new BosClientConfiguration();
//        config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID, SECRET_ACCESS_ID));
//        config.setEndpoint(ENDPOINT);
//        BosClient client_1 = new BosClient(config);
//        client = client_1;
//        return client;
//    }
//    BosClient client;
//    public void getCoverage(){
//        GetObjectRequest getObjectRequest = new GetObjectRequest("ogebos","DEM/ALOS_PALSAR-DEM12.5/n000-n009/n000e009_dem12.5.tif");
//        client.getObject(getObjectRequest, new File("/path/to/file","filename"));
//    }
//}
