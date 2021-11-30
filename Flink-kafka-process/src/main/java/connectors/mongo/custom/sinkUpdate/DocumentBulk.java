package connectors.mongo.custom.sinkUpdate;
import org.bson.Document;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
/**
 * DocumentBulk is buffered {@link Document} in memory, which would be written to MongoDB in a single transaction.
 * Due to execution efficiency, each DocumentBulk maybe be limited to a maximum size, typically 1,000 documents. But
 * for the transactional mode, the maximum size should not be respected because all that data must be written in one
 * transaction.
 * DocumentBulk在内存中被缓冲为Document，它将在一个事务中被写入MongoDB。由于执行效率的原因，
 * 每个DocumentBulk可能被限制在最大大小，通常是1000个文档。但是对于事务模式，不应该考虑最大大小，
 * 因为所有数据都必须在一个事务中写入。
 **/
@SuppressWarnings("checkstyle:RegexpMultiline")
@NotThreadSafe
public class DocumentBulk implements Serializable {
    private List<Document> bufferedDocuments;
    private final long maxSize;
    private static final int BUFFER_INIT_SIZE = Integer.MAX_VALUE;
    public DocumentBulk() {
        this(BUFFER_INIT_SIZE);
    }
    public DocumentBulk(long maxSize) {
        this.maxSize = maxSize;
        bufferedDocuments = new ArrayList<>(1024);
    }

    public int add(Document document) {
        if (bufferedDocuments.size() == maxSize) {
            throw new IllegalStateException("DocumentBulk is already full");
        }
        bufferedDocuments.add(document);
        return bufferedDocuments.size();
    }

    int size() {
        return bufferedDocuments.size();
    }

    boolean isFull() {
        return bufferedDocuments.size() >= maxSize;
    }

     List<Document> getDocuments() {
        return bufferedDocuments;
    }

    @Override
    public String toString() {
        return "DocumentBulk{" +
                "bufferedDocuments=" + bufferedDocuments +
                ", maxSize=" + maxSize +
                '}';
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (!(o instanceof DocumentBulk)) {
            return false;
        }
        DocumentBulk bulk = (DocumentBulk) o;
        return maxSize == bulk.maxSize &&
                Objects.equals(bufferedDocuments, bulk.bufferedDocuments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bufferedDocuments, maxSize);
    }
}
