package whu.edu.cn.ogc.entity.process;

import com.alibaba.fastjson.JSONObject;

public class ProcessRequestBody {
    private JSONObject inputs;
    private JSONObject outputs;
    private String response;
    private JSONObject subscriber;

    public JSONObject getInputs() {
        return inputs;
    }

    public void setInputs(JSONObject inputs) {
        this.inputs = inputs;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public JSONObject getOutputs() {
        return outputs;
    }

    public void setOutputs(JSONObject outputs) {
        this.outputs = outputs;
    }

    public JSONObject getSubscriber() {
        return subscriber;
    }

    public void setSubscriber(JSONObject subscriber) {
        this.subscriber = subscriber;
    }
}
