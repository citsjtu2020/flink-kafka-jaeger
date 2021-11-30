package connectors.mongo.custom.sinkUpdate;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.result.InsertManyResult;
import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/*实现事务的支持*/
public class CommittableTransaction implements TransactionBody<Integer>, Serializable {
    private static final int BUFFER_INIT_SIZE = 1024;
    private final MongoCollection<Document> collection;
    private List<Document> bufferedDocuments = new ArrayList<>(BUFFER_INIT_SIZE);

    public CommittableTransaction(MongoCollection<Document> collection, List<Document> documents) {
        this.collection = collection;
        this.bufferedDocuments.addAll(documents);
    }

    @Override
    public Integer execute() {
        InsertManyResult result = collection.insertMany(bufferedDocuments);
        return result.getInsertedIds().size();
    }
}
