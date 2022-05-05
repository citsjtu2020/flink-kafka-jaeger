import connectors.mongo2.serde.DocumentSerializer;
import org.bson.Document;

public class AIOPSGraphSpanDocumentSerializer implements DocumentSerializer<AIOPSGraphSpan> {

    @Override
    public Document serialize(AIOPSGraphSpan object) {
//        return null;
        Document document = new Document();
//        document.append("timestamp",objec)
        document.append("timestamp",object.getTimestamp());
        document.append("start",object.getStart());
        document.append("end",object.getEnd());
        document.append("type",object.getProtocol());
        document.append("count",object.getCount());
        return document;
    }
}
