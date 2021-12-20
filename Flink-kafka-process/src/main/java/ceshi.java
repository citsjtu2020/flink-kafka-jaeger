import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class ceshi {

    public static void main(String[] args) throws JsonProcessingException {
        String json = "{\"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"3bd72513072bc36b\", \"flags\": 1, \"operationName\": \"/geo.Geo/Nearby\", \"references\": [{\"refType\": \"CHILD_OF\", \"traceID\": \"07ee4f006c3cc562\", \"spanID\": \"242d4a802b9eb2ce\"}], \"startTime\": 1633607526581555, \"startTimeMillis\": 1633607526581, \"duration\": 400, \"tags\": [{\"key\": \"span.kind\", \"type\": \"string\", \"value\": \"client\"}, {\"key\": \"component\", \"type\": \"string\", \"value\": \"gRPC\"}, {\"key\": \"internal.span.format\", \"type\": \"string\", \"value\": \"proto\"}], \"logs\": [], \"process\": {\"serviceName\": \"search\", \"tags\": [{\"key\": \"jaeger.version\", \"type\": \"string\", \"value\": \"Go-2.11.2\"}, {\"key\": \"hostname\", \"type\": \"string\", \"value\": \"search-6488494995-ktm47\"}, {\"key\": \"ip\", \"type\": \"string\", \"value\": \"10.64.0.27\"}]}}";
        JsonNode arrNode = new ObjectMapper().readTree(json).get("references");
        System.out.println(arrNode.isArray());
        System.out.println(arrNode.get(0).get("spanID").asText().trim());

        Tuple9<String, String, String, String, Long, Long, String, String, String> s=new Tuple9<>();

    }

}