import connectors.mongo2.serde.DocumentSerializer;
import org.bson.Document;

public class QPSsDocumentSerializer implements DocumentSerializer<QPSs> {

    @Override
    public Document serialize(QPSs object) {
//        return null;
        Document document = new Document();
        document.append("timestamp",object.getTimestamp());
        document.append("api",object.getApi());
        document.append("count",object.getCount());
        document.append("mean",object.getMeandur());
        document.append("max",object.getMaxdur());
        document.append("min",object.getMindur());
        document.append("p50",object.getP50dur());
        document.append("p95",object.getP95dur());
        document.append("p99",object.getP99dur());
        document.append("std",object.getStd());
//        System.out.println(document);
        return document;
    }
}
