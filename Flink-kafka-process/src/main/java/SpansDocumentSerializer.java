import connectors.mongo.serde.DocumentSerializer;
import org.bson.Document;

public class SpansDocumentSerializer implements DocumentSerializer<Spans> {
    @Override
    public Document serialize(Spans object) {
//        return null;
//        this.api = api;
//        this.parent = parent;
//        this.count = count;
//        this.timestamp = timestamp;
        Document document = new Document();
        document.append("api",object.getApi());
        document.append("parent",object.getParent());
        document.append("count",object.getCount());
        document.append("timestamp",object.getTimestamp());
        return document;
    }
}
