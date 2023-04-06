package whu.edu.cn.ogc.entity.process;

import java.util.List;
import java.util.Map;

public class Process {
    private String id;
    private String version;
    private String title;
    private String description;
    private List<String> keywords;
    private List<Link> links;
    private List<String> jobControlOptions;
    private List<String> outputTransmission;
    private Map<String, Input> inputs;
    private Map<String, Output> outputs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

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

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    public List<Link> getLinks() {
        return links;
    }

    public void setLinks(List<Link> links) {
        this.links = links;
    }

    public List<String> getJobControlOptions() {
        return jobControlOptions;
    }

    public void setJobControlOptions(List<String> jobControlOptions) {
        this.jobControlOptions = jobControlOptions;
    }

    public List<String> getOutputTransmission() {
        return outputTransmission;
    }

    public void setOutputTransmission(List<String> outputTransmission) {
        this.outputTransmission = outputTransmission;
    }

    public Map<String, Input> getInputs() {
        return inputs;
    }

    public void setInputs(Map<String, Input> inputs) {
        this.inputs = inputs;
    }

    public Map<String, Output> getOutputs() {
        return outputs;
    }

    public void setOutputs(Map<String, Output> outputs) {
        this.outputs = outputs;
    }
}
