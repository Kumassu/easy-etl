package song.pan.etl.service.domain;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public enum ConflictStrategy {

    /**
     * Drop the existing target table,
     * create a new table for loading
     */
    DROP,

    /**
     * Truncate the existing target table,
     * before loading data into it
     */
    TRUNCATE,

    /**
     * Load data to the existing target table
     */
    CONTINUE,

    /**
     * Abort task
     */
    EXIT,
    ;


}
