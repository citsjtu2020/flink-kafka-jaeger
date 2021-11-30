package connectors.mongo.custom.sinkUpdate;

import connectors.mongo.config.SinkConfiguration;
import connectors.mongo.internal.connection.MongoClientProvider;
import connectors.mongo.serde.DocumentSerializer;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.bson.Document;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
/*
* Writer for MongoDB sink.
** <InputT> – 接收器写入器输入的类型
 * <CommT> – 提交由接收器暂存的数据所需的信息类型
 * <WriterStateT> - 作者状态的类型
* */
public class MongoBulkWriter<IN> implements SinkWriter<IN, DocumentBulk, DocumentBulk> {
    private final MongoClientProvider collectionProvider;
    //    private transient MongoCollection<Document> collection;
    private MongoCollection<Document> collection;
    private DocumentBulk currentBulk;
    private final List<DocumentBulk> pendingBulks = new ArrayList<>();
    private DocumentSerializer<IN> serializer;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture scheduledFuture;
    private transient volatile Exception flushException;
    private final long maxSize;
    private final boolean flushOnCheckpoint;
    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);
    private transient volatile boolean closed = false;

    private final String key;
    //private static final Logger LOGGER = LoggerFactory.getLogger(MongoBulkWriter.class);

    @SuppressWarnings("AlibabaThreadPoolCreation")
    public MongoBulkWriter(MongoClientProvider collectionProvider,
                           DocumentSerializer<IN> serializer,
                           SinkConfiguration configuration,
                           String key){
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
        //前面传来 定义缓存大小的值
        this.maxSize = configuration.getBulkFlushSize();
        this.currentBulk = new DocumentBulk(maxSize);
        this.flushOnCheckpoint = configuration.isFlushOnCheckpoint();
        this.key = key;

        if (!flushOnCheckpoint && configuration.getBulkFlushInterval() > 0){
            //noinspection AlibabaThreadPoolCreation
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("mongodb-bulk-writer"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongoBulkWriter.this) {
                                    if (!closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            configuration.getBulkFlushInterval(),
                            configuration.getBulkFlushInterval(),
                            TimeUnit.MILLISECONDS);
        }
    }
    public void initializeState(List<DocumentBulk> recoveredBulks){
        collection = collectionProvider.getDefaultCollection();
        for (DocumentBulk bulk: recoveredBulks) {
            for (Document document: bulk.getDocuments()) {
                rollBulkIfNeeded();
                currentBulk.add(document);
            }
        }
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }

    private synchronized void rollBulkIfNeeded(boolean force) {
        if (force || currentBulk.isFull()) {
            pendingBulks.add(currentBulk);
            currentBulk = new DocumentBulk(maxSize);
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
        }
    }

    @Override
    public void write(IN in, Context context) throws IOException {
        checkFlushException();
        rollBulkIfNeeded();
        //拿到我们source的数据将其写入到我们的内存缓存中
        currentBulk.add(serializer.serialize(in));
    }

    @Override
    public List<DocumentBulk> prepareCommit(boolean flush) throws IOException {
//        return null;
        if (flushOnCheckpoint || flush) {
            rollBulkIfNeeded(true);
        }
        //准备好要提交的数据
        return pendingBulks;
    }

    @Override
    public List<DocumentBulk> snapshotState() throws IOException {
//        return null;
        List<DocumentBulk> inProgressAndPendingBulks = new ArrayList<>(1);
        inProgressAndPendingBulks.add(currentBulk);
        inProgressAndPendingBulks.addAll(pendingBulks);
        pendingBulks.clear();
        return inProgressAndPendingBulks;
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }

     /**
     * Flush by non-transactional bulk write, which may result in data duplicates after multiple tries.
     * There may be concurrent flushes when concurrent checkpoints are enabled.
     *
     * We manually retry write operations, because the driver doesn't support automatic retries for some MongoDB
     * setups (e.g. standalone instances). TODO: This should be configurable in the future.
     */
     private synchronized void flush() {
         if(!closed){
             ensureConnection();
             retryPolicy.reset();
             Iterator<DocumentBulk> iterator = pendingBulks.iterator();
             while (iterator.hasNext()){
                 DocumentBulk bulk = iterator.next();
                 do{
                     try {
                        // ordered, non-bypass mode
                        Document query = new Document().append("_id", key);
                        UpdateOptions options = new UpdateOptions().upsert(true);
                        collection.updateMany(query,bulk.getDocuments(),options);
                        iterator.remove();
                        break;
                     }catch (MongoException e) {
                        // maybe partial failure
                        //LOGGER.error("Failed to flush data to MongoDB", e);
                    }
                 }while (!closed && retryPolicy.shouldBackoffRetry());
             }
         }
     }
     private void ensureConnection() {
         try {
             collection.listIndexes();
         } catch (MongoException e) {
             //LOGGER.warn("Connection is not available, try to reconnect", e);
             collectionProvider.recreateClient();
         }
     }
    @NotThreadSafe
    class RetryPolicy {
        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }
}
