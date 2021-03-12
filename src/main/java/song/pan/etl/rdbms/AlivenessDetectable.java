package song.pan.etl.rdbms;

/**
 * Capable of detecting the database's aliveness
 *
 * @author Song Pan
 * @version 1.0.0
 */
public interface AlivenessDetectable {


    /**
     * Check if current server is alive
     * @return {@code true} if alive, {@code false} otherwise
     */
    boolean isServerAlive();


    /**
     * Check if specified database is available
     * @param name database/catalog name
     * @return {@code true} if database available, {@code false} otherwise
     */
    boolean isDatabaseAvailable(String name);

}
