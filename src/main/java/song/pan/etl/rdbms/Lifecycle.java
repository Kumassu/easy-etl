package song.pan.etl.rdbms;

import song.pan.etl.common.exception.DisconnectException;
import song.pan.etl.common.exception.LoginException;

/**
 * Define life cycles of database server
 *
 * @author Song Pan
 * @version 1.0.0
 */
public interface Lifecycle {


    /**
     * Initialize {@link RdbmsServer}
     * @param properties initialization properties
     */
    void init(ConnectionProperties properties);


    /**
     * Try to connect current server,
     * throw {@link LoginException} if fail
     */
    void connect();


    /**
     * Close the database connection,
     * throw {@link DisconnectException} if fail
     */
    void disconnect();


}
