package song.pan.etl.service.domain;

import java.sql.ResultSet;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public enum PaginationStrategy {

    /**
     * Extract data by {@link ResultSet#next()}
     */
    CURSOR,


    /**
     * Order the query by pagination key and extract certain
     * number of top rows from the ordered results as one page,
     * use the page's last row as next query's start boundary
     */
    DEPENDENT,


    /**
     * Find pagination key's max and min value first,
     * then averagely split the query by a fixed step
     */
    DISTRIBUTE,


    /**
     * Copy the query results to a new table and
     * generate an auto-increased id as pagination key
     */
    GENERATE,
    ;


}
