package whu.edu.cn.geocube.application.tritonClient.examples;

import com.google.common.collect.Lists;
import whu.edu.cn.geocube.application.tritonClient.*;
import whu.edu.cn.geocube.application.tritonClient.pojo.DataType;

import java.io.IOException;
import java.util.List;

public class ModelInference {
    public static float[] byteToFloatOPT(byte[] data1, byte[] data2, byte[] data3, byte[] data4) {
        float[] result = new float[data1.length * 4];
        for (int i = 0; i < data1.length; i++) {
            int dataInt1 = data1[i];
            int dataInt2 = data2[i];
            int dataInt3 = data3[i];
            int dataInt4 = data4[i];
            result[i] = (float) (((dataInt1 + 127) / 255.0 - (-0.8287161)) / 0.23686765);
            result[i + 512 * 512] = (float) (((dataInt2 + 127) / 255.0 - (-0.9534326)) / 0.2726873);
            result[i + 2 * 512 * 512] = (float) (((dataInt3 + 127) / 255.0 - (-0.10713897)) / 0.53583276);
            result[i + 3 * 512 * 512] = (float) (((dataInt4 + 127) / 255.0 - (-0.10713897)) / 0.53583276);
        }
        return result;
    }

    public static float[] byteToFloatSAR(byte[] data) {
        float[] result = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            int dataInt = data[i];
            result[i] = (float) (((dataInt + 127) / 255.0 - 1.1816614) / 0.8776201);
        }
        return result;
    }

    public static float[] processOneTile(float[] inputOPT, float[] inputSAR) {
        try {
            boolean isBinary = true;
            InferInput inputIdOPT = new InferInput("input", new long[]{1L, 4L, 512L, 512L}, DataType.FP32);
            inputIdOPT.setData(inputOPT, isBinary);
            InferInput inputIdSAR = new InferInput("input.189", new long[]{1L, 1L, 512L, 512L}, DataType.FP32);
            inputIdSAR.setData(inputSAR, isBinary);

            List<InferInput> inputs = Lists.newArrayList(inputIdOPT, inputIdSAR);
            List<InferRequestedOutput> outputs = Lists.newArrayList(new InferRequestedOutput("output", isBinary));

            InferenceServerClient client = new InferenceServerClient("125.220.153.57:8000", 5000, 5000);
            System.out.println("yes!!!!");
            InferResult result = client.infer("OSNet_ConvNeXt-N_OSDataset_69", inputs, outputs);
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            return result.getOutputAsFloat("output");
        } catch (IOException | InferenceException e) {
            e.printStackTrace();
            return null;
        }
    }
}
