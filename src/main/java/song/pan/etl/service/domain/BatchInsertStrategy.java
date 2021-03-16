package song.pan.etl.service.domain;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public enum BatchInsertStrategy {

    /**
     * {@link java.sql.PreparedStatement} use question mark as placeholder
     */
    JDBC,

    /**
     * {@link org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate}
     * use field name as placeholder
     */
    SPRING

}
