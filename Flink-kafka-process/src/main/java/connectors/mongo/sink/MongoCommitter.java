package connectors.mongo.sink;

import connectors.mongo.internal.connection.MongoClientProvider;
import org.apache.flink.api.connector.sink.Committer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
/*
* MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a transaction is
* not recommended to be large.
* */
public class MongoCommitter implements Committer<DocumentBulk>{
    private final MongoClient client;

    private final MongoCollection<Document> collection;

    private final static Logger LOGGER = LoggerFactory.getLogger(MongoCommitter.class);
    private TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();

    public MongoCommitter(MongoClientProvider clientProvider) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
    }

    @Override
    public List<DocumentBulk> commit(List<DocumentBulk> committables) throws IOException{
        ClientSession session = client.startSession();
        List<DocumentBulk> failedBulk = new ArrayList<>();
        for (DocumentBulk bulk : committables){
            if (bulk.getDocuments().size() > 0) {
                CommittableTransaction transaction = new CommittableTransaction(collection, bulk.getDocuments());
                try {
                    session.withTransaction(transaction, txnOptions);
                } catch (Exception e) {
                    // save to a new list that would be retried
                    LOGGER.error("Failed to commit with Mongo transaction", e);
                    failedBulk.add(bulk);
                    session.close();
                }
            }
        }
        return failedBulk;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
