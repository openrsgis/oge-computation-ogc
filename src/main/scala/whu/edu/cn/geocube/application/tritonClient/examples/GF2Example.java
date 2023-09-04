package whu.edu.cn.geocube.application.tritonClient.examples;

import com.google.common.collect.Lists;
import whu.edu.cn.geocube.application.tritonClient.*;
import whu.edu.cn.geocube.application.tritonClient.pojo.DataType;

import java.io.IOException;
import java.net.URL;
import java.util.List;

public class GF2Example {

    public static float[] byteToFloat(byte[] data1, byte[] data2, byte[] data3) {
        float[] result = new float[data1.length * 3];
        for (int i = 0; i < data1.length; i++) {
            int dataInt1 = data1[i];
            int dataInt2 = data2[i];
            int dataInt3 = data3[i];
            result[i] = (float) ((dataInt1 + 127)/255.0);
            result[i + 512 * 512] = (float) ((dataInt2 + 127)/255.0);
            result[i + 2 * 512 * 512] = (float) ((dataInt3 + 127)/255.0);
        }
        return result;
    }

    public static float[] processOneTile(float[] inputIdsData) {
        try {
            boolean isBinary = true;
            InferInput inputIds = new InferInput("input0", new long[]{1L, 3L, 512L, 512L}, DataType.FP32);
            //float[] inputIdsData = new float[786432];
            //float[] inputIdsData = readPNG("D:/000011.png");
            inputIds.setData(inputIdsData, isBinary);

            List<InferInput> inputs = Lists.newArrayList(inputIds);
            List<InferRequestedOutput> outputs = Lists.newArrayList(new InferRequestedOutput("output0", isBinary));

            ClassLoader classLoader = InferenceServerClient.class.getClassLoader();
            URL resource = classLoader.getResource("org/apache/http/message/BasicLineFormatter.class");
            System.out.println(resource);

            InferenceServerClient client = new InferenceServerClient("125.220.153.57:8000", 5000, 5000);
            System.out.println("yes!!!!");
            InferResult result = client.infer("crop_recog_swin_onnx", inputs, outputs);
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            float[] output = result.getOutputAsFloat("output0");

            return output;
        } catch (IOException | InferenceException e) {
            e.printStackTrace();
            return null;
        }
    }
}