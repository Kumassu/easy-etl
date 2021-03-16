package song.pan.etl.rdbms;

import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Capable of creating {@link DataSource} and {@link JdbcTemplate}
 *
 * @author Song Pan
 * @version 1.0.0
 */
public interface EasyConnect {


    ConnectionProperties getConnectionProperties();


    DataSource getDataSource();


    JdbcTemplate getJdbcTemplate();

}
