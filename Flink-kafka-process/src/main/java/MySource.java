import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;

public class MySource implements SourceFunction<String> {
    private long count = 0L;
    private boolean isRunning = true;
    private ArrayList<JSONObject> examples = new ArrayList<>();
//    {"traceID": "07ee4f006c3cc562",
//    "spanID": "242d4a802b9eb2ce",
//    "flags": 1,
//    "operationName": "/search.Search/Nearby",
//    "references": [
//      {"refType": "CHILD_OF", "traceID": "07ee4f006c3cc562", "spanID": "67263f65c08da1f1"}
//      ],
//      "startTime": 1633607526581551,
//      "startTimeMillis": 1633607526581,
//      "duration": 618,
//      "tags": [
//      {"key": "span.kind", "type": "string", "value": "server"}, component
//      {"key": "component", "type": "string", "value": "gRPC"},
//      {"key": "internal.span.format", "type": "string", "value": "proto"}
//      ],
//      "logs": [],
//      "process": {
//      "serviceName": "search",
//      "tags": [
//      {"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"},
//      {"key": "hostname", "type": "string", "value": "search-6488494995-ktm47"},
//      {"key": "ip", "type": "string", "value": "10.64.0.27"}
//      ]
//      }
//      }
//{"traceID": "07ee4f006c3cc562", "spanID": "3bd72513072bc36b", "flags": 1, "operationName": "/geo.Geo/Nearby", "references": [{"refType": "CHILD_OF", "traceID": "07ee4f006c3cc562", "spanID": "242d4a802b9eb2ce"}], "startTime": 1633607526581555, "startTimeMillis": 1633607526581, "duration": 400, "tags": [{"key": "span.kind", "type": "string", "value": "client"}, {"key": "component", "type": "string", "value": "gRPC"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "search", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "search-6488494995-ktm47"}, {"key": "ip", "type": "string", "value": "10.64.0.27"}]}}
//{"traceID": "18247c2cdf616b3a", "spanID": "71bf320f19da2fc2", "flags": 1, "operationName": "/profile.Profile/GetProfiles", "references": [{"refType": "CHILD_OF", "traceID": "18247c2cdf616b3a", "spanID": "7da5f894f42c9459"}], "startTime": 1633607526581622, "startTimeMillis": 1633607526581, "duration": 1, "tags": [{"key": "span.kind", "type": "string", "value": "server"}, {"key": "component", "type": "string", "value": "gRPC"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "profile", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "profile-5c7445749d-nn8k8"}, {"key": "ip", "type": "string", "value": "10.64.0.26"}]}}
//{"traceID": "197bbc0205347589", "spanID": "62074ffaf64983ae", "flags": 1, "operationName": "/search.Search/Nearby", "references": [{"refType": "CHILD_OF", "traceID": "197bbc0205347589", "spanID": "2664161d681e867a"}], "startTime": 1633607526581629, "startTimeMillis": 1633607526581, "duration": 727, "tags": [{"key": "span.kind", "type": "string", "value": "server"}, {"key": "component", "type": "string", "value": "gRPC"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "search", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "search-6488494995-ktm47"}, {"key": "ip", "type": "string", "value": "10.64.0.27"}]}}
//{"traceID": "197bbc0205347589", "spanID": "2a96d6320b3e4cae", "flags": 1, "operationName": "/geo.Geo/Nearby", "references": [{"refType": "CHILD_OF", "traceID": "197bbc0205347589", "spanID": "62074ffaf64983ae"}], "startTime": 1633607526581632, "startTimeMillis": 1633607526581, "duration": 488, "tags": [{"key": "span.kind", "type": "string", "value": "client"}, {"key": "component", "type": "string", "value": "gRPC"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "search", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "search-6488494995-ktm47"}, {"key": "ip", "type": "string", "value": "10.64.0.27"}]}}
//{"traceID": "07ee4f006c3cc562", "spanID": "07ee4f006c3cc562", "flags": 1, "operationName": "HTTP GET /hotels", "references": [], "startTime": 1633607526581681, "startTimeMillis": 1633607526581, "duration": 1781, "tags": [{"key": "sampler.type", "type": "string", "value": "probabilistic"}, {"key": "sampler.param", "type": "float64", "value": "0.3"}, {"key": "span.kind", "type": "string", "value": "server"}, {"key": "http.method", "type": "string", "value": "GET"}, {"key": "http.url", "type": "string", "value": "http://localhost:5000/hotels?inDate=2015-04-16&outDate=2015-04-20&lat=38.247&lon=-122.125"}, {"key": "component", "type": "string", "value": "net/http"}, {"key": "http.status_code", "type": "int64", "value": "200"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "frontend", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "frontend-555d89cd86-lgn82"}, {"key": "ip", "type": "string", "value": "10.64.3.27"}]}}
//{"traceID": "07ee4f006c3cc562", "spanID": "67263f65c08da1f1", "flags": 1, "operationName": "/search.Search/Nearby", "references": [{"refType": "CHILD_OF", "traceID": "07ee4f006c3cc562", "spanID": "07ee4f006c3cc562"}], "startTime": 1633607526581700, "startTimeMillis": 1633607526581, "duration": 833, "tags": [{"key": "span.kind", "type": "string", "value": "client"}, {"key": "component", "type": "string", "value": "gRPC"}, {"key": "internal.span.format", "type": "string", "value": "proto"}], "logs": [], "process": {"serviceName": "frontend", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "frontend-555d89cd86-lgn82"}, {"key": "ip", "type": "string", "value": "10.64.3.27"}]}}

////{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"3bd72513072bc36b\", \"flags\": 1, \"operationName\": \"/geo.Geo/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"242d4a802b9eb2ce\"}], \"startTime\": 1633607526581555, \"startTimeMillis\": 1633607526581, \"duration\": 400, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}
////{\"traceID\": \"18247c2cdf616b3a\", \"spanID\": \"71bf320f19da2fc2\", \"flags\": 1, \"operationName\": \"/profile.Profile/GetProfiles\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"18247c2cdf616b3a\", \"spanID\": \"7da5f894f42c9459\"}], \"startTime\": 1633607526581622, \"startTimeMillis\": 1633607526581, \"duration\": 1, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"profile\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"profile-5c7445749d-nn8k8\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.26\"}]}}
////{\"traceID\": \"197bbc0205347589\", \"spanID\": \"62074ffaf64983ae\", \"flags\": 1, \"operationName\": \"/search.Search/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"197bbc0205347589\", \"spanID\": \"2664161d681e867a\"}], \"startTime\": 1633607526581629, \"startTimeMillis\": 1633607526581, \"duration\": 727, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}
////{\"traceID\": \"197bbc0205347589\", \"spanID\": \"2a96d6320b3e4cae\", \"flags\": 1, \"operationName\": \"/geo.Geo/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"197bbc0205347589\", \"spanID\": \"62074ffaf64983ae\"}], \"startTime\": 1633607526581632, \"startTimeMillis\": 1633607526581, \"duration\": 488, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}
//{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"07ee4f006c3cc562\",
// \"flags\": 1, \"operationName\": \"HTTP GET /hotels\",
// \"references\": [],
// \"startTime\": 1633607526581681,
// \"startTimeMillis\": 1633607526581,
// \"duration\": 1781,
// \"tags\": [
// {\"key\": \"sampler.type\", \"type\": \"string\",
// \"value\": \"probabilistic\"},
// {\"key\": \"sampler.param\", \"type\": \"float64\",
// \"value\": \"0.3\"},
// {\"key\": \"span.kind\", \"type\": \"string\",
// \"value\": \"server\"},
// {\"key\": \"http.method\", \"type\": \"string\", \"value\": \"GET\"}, {\"key\": \"http.url\", \"type\": \"string\", \"value\": \"http://localhost:5000/hotels?inDate=2015-04-16&outDate=2015-04-20&lat=38.247&lon=-122.125\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"net/http\"}, {\"key\": \"http.status_code\", \"type\": \"int64\", \"value\": \"200\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"frontend\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"frontend-555d89cd86-lgn82\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.3.27\"}]}}
//{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"67263f65c08da1f1\", \"flags\": 1, \"operationName\": \"/search.Search/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"07ee4f006c3cc562\"}], \"startTime\": 1633607526581700, \"startTimeMillis\": 1633607526581, \"duration\": 833, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"frontend\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"frontend-555d89cd86-lgn82\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.3.27\"}]}}


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        examples.add(JSON.parseObject("{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"242d4a802b9eb2ce\", \"flags\": 1, \"operationName\": \"/search.Search/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"67263f65c08da1f1\"}], \"startTime\": 1633607526581551, \"startTimeMillis\": 1633607526581, \"duration\": 618, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}"
        ));
        examples.add(JSON.parseObject("{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"3bd72513072bc36b\", \"flags\": 1, \"operationName\": \"/geo.Geo/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"242d4a802b9eb2ce\"}], \"startTime\": 1633607526581555, \"startTimeMillis\": 1633607526581, \"duration\": 400, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}"
        ));
        examples.add(JSON.parseObject("{\"traceID\": \"18247c2cdf616b3a\", \"spanID\": \"71bf320f19da2fc2\", \"flags\": 1, \"operationName\": \"/profile.Profile/GetProfiles\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"18247c2cdf616b3a\", \"spanID\": \"7da5f894f42c9459\"}], \"startTime\": 1633607526581622, \"startTimeMillis\": 1633607526581, \"duration\": 1, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"profile\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"profile-5c7445749d-nn8k8\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.26\"}]}}"));
        examples.add(JSON.parseObject("{\"traceID\": \"197bbc0205347589\", \"spanID\": \"62074ffaf64983ae\", \"flags\": 1, \"operationName\": \"/search.Search/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"197bbc0205347589\", \"spanID\": \"2664161d681e867a\"}], \"startTime\": 1633607526581629, \"startTimeMillis\": 1633607526581, \"duration\": 727, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}"));
        examples.add(JSON.parseObject("{\"traceID\": \"197bbc0205347589\", \"spanID\": \"2a96d6320b3e4cae\", \"flags\": 1, \"operationName\": \"/geo.Geo/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"197bbc0205347589\", \"spanID\": \"62074ffaf64983ae\"}], \"startTime\": 1633607526581632, \"startTimeMillis\": 1633607526581, \"duration\": 488, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}"));
        examples.add(JSON.parseObject("{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"07ee4f006c3cc562\", \"flags\": 1, \"operationName\": \"HTTP GET /hotels\", \"references\": [], \"startTime\": 1633607526581681, \"startTimeMillis\": 1633607526581, \"duration\": 1781, \"tags\": [{\"key\": \"sampler.type\", \"type\": \"string\", \"value\": \"probabilistic\"}, {\"key\": \"sampler.param\", \"type\": \"float64\", \"value\": \"0.3\"}, {\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"server\"}, {\"key\": \"http.method\", \"type\": \"string\", \"value\": \"GET\"}, {\"key\": \"http.url\", \"type\": \"string\", \"value\": \"http://localhost:5000/hotels?inDate=2015-04-16&outDate=2015-04-20&lat=38.247&lon=-122.125\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"net/http\"}, {\"key\": \"http.status_code\", \"type\": \"int64\", \"value\": \"200\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"frontend\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"frontend-555d89cd86-lgn82\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.3.27\"}]}}"
        ));
        examples.add(JSON.parseObject("{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"67263f65c08da1f1\", \"flags\": 1, \"operationName\": \"/search.Search/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"07ee4f006c3cc562\"}], \"startTime\": 1633607526581700, \"startTimeMillis\": 1633607526581, \"duration\": 833, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"frontend\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"frontend-555d89cd86-lgn82\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.3.27\"}]}}"
        ));
        int lens = examples.size();
        long basetime = Long.parseLong(examples.get(0).get("startTimeMillis").toString());
        while (isRunning){

            JSONObject c = examples.get((int)count%lens);
            c.put("startTimeMillis",basetime+3000*count);
            ctx.collect(c.toString());
            count += 1;
            System.out.println(count);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


}


