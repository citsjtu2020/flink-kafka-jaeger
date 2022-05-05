package connectors.mongo2.internal.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.Serializable;

/**
 * Provided for initiate and recreate {@link MongoClient}.
 **/
public interface MongoClientProvider extends Serializable {

    /**
     * Create one or get the current {@link MongoClient}.
     * @return Current {@link MongoClient}.
     */
    MongoClient getClient();

    /**
     * Get the default database.
     * @return Current {@link MongoDatabase}.
     */
    MongoDatabase getDefaultDatabase();

    /**
     * Get the default collection.
     * @return Current {@link MongoCollection}.
     */
    MongoCollection<Document> getDefaultCollection();

    /**
     * Recreate a client. Used typically when a connection is timed out or lost.
     * @return A new {@link MongoClient}.
     */
    MongoClient recreateClient();

    /**
     * Close the underlying MongoDB connection.
     */
    void close();
}
