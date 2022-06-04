package model;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.ByteIterator;
import com.google.protobuf.util.Timestamps;
import io.jaegertracing.api_v2.Model;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.ListIterator;

public class ProtoUnmarshaler extends AbstractDeserializationSchema<Spanraw> {

    private static final long serialVersionUID = 7952979116702613945L;

    @Override
    public Spanraw deserialize(byte[] message) throws IOException {
        Model.Span protoSpan = Model.Span.parseFrom(message);
        return fromProto(protoSpan);
    }
////{"traceID": "07ee4f006c3cc562",
// "spanID": "3bd72513072bc36b",
// "flags": 1,
// "operationName": "/geo.Geo/Nearby",
// "references": [
// {"refType": "CHILD_OF", "traceID": "07ee4f006c3cc562", "spanID": "242d4a802b9eb2ce"}],
// "startTime": 1633607526581555,
// "startTimeMillis": 1633607526581,
// "duration": 400,
// "tags": [{"key": "span.kind", "type": "string", "value": "client"},
// {"key": "component", "type": "string", "value": "gRPC"},
// {"key": "internal.span.format", "type": "string", "value": "proto"}],
// "logs": [],
// "process": {"serviceName": "search", "tags": [{"key": "jaeger.version", "type": "string", "value": "Go-2.11.2"}, {"key": "hostname", "type": "string", "value": "search-6488494995-ktm47"}, {"key": "ip", "type": "string", "value": "10.64.0.27"}]}}
    private Spanraw fromProto(Model.Span protoSpan) {
        Spanraw span = new Spanraw();
        span.setTraceId(asHexString(protoSpan.getTraceId()));
        span.setSpanId(asHexString(protoSpan.getSpanId()));
        span.setOperationName(protoSpan.getOperationName());
        span.setDuration((protoSpan.getDuration().getSeconds()*1e9+protoSpan.getDuration().getNanos())/1000.0);
        span.setServiceName(protoSpan.getProcess().getServiceName());
//        protoSpan.getProcess().
        span.setStartTimeMicros(Timestamps.toMicros(protoSpan.getStartTime()));
//        Timestamps.toMillis(protoSpan.getStartTime())
        span.setStartTimeMillis((long)(Timestamps.toMicros(protoSpan.getStartTime())/1000));
//        span.setStartTimeMillis();
//        java.util.List<SpanRef> references_
        java.util.List<Model.SpanRef> referlist = protoSpan.getReferencesList();
        span.setParentId("");
//traceId
//        span.spanId
        if (referlist.size() <= 0 || span.getTraceId().equals(span.getSpanId())){
//            span.setSpanId("");
            span.setParentId("");
        }else{
            for (Model.SpanRef tmpref : referlist) {
                //                referlist.get(0).getRefType().equals(Model.SpanRefType.CHILD_OF){
//
//            }
                if (tmpref.getRefType().equals(Model.SpanRefType.CHILD_OF)) {
                    span.setParentId(asHexString(tmpref.getSpanId()));
//                    span.parentId = asHexString(tmpref.getSpanId());
                    break;
                }
            }
//            span.parentId
            if (span.getParentId().equals("")){
                for (Model.SpanRef tmpref : referlist) {
                    //                referlist.get(0).getRefType().equals(Model.SpanRefType.CHILD_OF){
//
//            }
                    if (tmpref.getRefType().equals(Model.SpanRefType.FOLLOWS_FROM)) {
                        span.setParentId(asHexString(tmpref.getSpanId()));
//                        span.parentId = asHexString(tmpref.getSpanId());
                        break;
                    }
                }
            }
        }
//        span.parentId = protoSpan.getReferences().
//        span.tags = new HashMap<>();
        span.InitTags();
        for (Model.KeyValue kv : protoSpan.getTagsList()) {
            if (!Model.ValueType.STRING.equals(kv.getVType())) {
                continue;
            }
            String value = kv.getVStr();
            if (value != null) {
//                span.tags.put(kv.getKey(), value);
                span.addTags(kv.getKey(), value);
            }
        }
        for (Model.KeyValue kv :protoSpan.getProcess().getTagsList()){
            if (!Model.ValueType.STRING.equals(kv.getVType())) {
                continue;
            }
            String value = kv.getVStr();
            if (value != null) {
//                span.tags.put(kv.getKey(), value);
                span.addTags(kv.getKey(), value);
            }
        }
        return span;
    }

    private static final String HEXES = "0123456789ABCDEF";

    private String asHexString(ByteString id) {
        ByteIterator iterator = id.iterator();
        StringBuilder out = new StringBuilder();
        while (iterator.hasNext()) {
            byte b = iterator.nextByte();
            out.append(HEXES.charAt((b & 0xF0) >> 4)).append(HEXES.charAt((b & 0x0F)));
        }
        return out.toString();
    }
}