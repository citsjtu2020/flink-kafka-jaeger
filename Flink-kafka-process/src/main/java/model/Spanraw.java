package model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Spanraw implements Serializable {
    public String traceId;
    public String spanId;
    public String serviceName;
    public String operationName;
    public long startTimeMicros;
    public long startTimeMillis;
    public String parentId;
    public double duration;
//    public
    public Map<String, String> tags;

    public double getDuration() {
        return this.duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }

    public String getOperationName() {
        return this.operationName;
    }

    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public long getStartTimeMicros() {
        return this.startTimeMicros;
    }

    public void setStartTimeMicros(long startTimeMicros) {
        this.startTimeMicros = startTimeMicros;
    }

    public long getStartTimeMillis() {
        return this.startTimeMillis;
    }

    public String getParentId() {
        return this.parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public Map<String, String> getTags() {
        return this.tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public void InitTags(){
        this.tags = new HashMap<>();
    }

    public void addTags(String key,String value){
        this.tags.put(key,value);
    }

    public void removeTags(String key){
        this.tags.remove(key);
    }

    public String getTagsItem(String key){

        return this.tags.getOrDefault(key, "");


    }

    public boolean containTagItem(String key){
        return this.tags.containsKey(key);
    }



    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public String getSpanId() {
        return this.spanId;
    }

    public String getTraceId() {
        return this.traceId;
    }

    public void setStartTimeMillis(long startTimeMillis) {
        this.startTimeMillis = startTimeMillis;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }


}

