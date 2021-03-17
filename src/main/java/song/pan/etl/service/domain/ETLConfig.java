package song.pan.etl.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.element.Table;

import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class ETLConfig {

    @JsonIgnore
    private RdbmsServer sourceServer;

    private Table sourceTable;

    @JsonIgnore
    private RdbmsServer destServer;

    private Table destTable;

    private String query;

    /**
     * SQL scripts to execute in source server before ETL
     */
    private List<String> sourceServerPreScripts;


    /**
     * SQL scripts to execute in source server after ETL
     */
    private List<String> sourceServerPostScripts;


    /**
     * SQL scripts to execute in destination server before ETL
     */
    private List<String> destServerPreScripts;


    /**
     * SQL scripts to execute in source server after ETL
     */
    private List<String> destServerPostScripts;


    /**
     * SQL scripts to execute in destination server after target table created
     */
    private List<String> targetTablePostScripts;


    private List<String> paginationKeys;


    private int channel;

    private String replicaDatabase;
    private Table replicaTable;

    private boolean dropTargetTable;

    /**
     * If target table exists in destination server, a substitute table will be created
     * as loading buffer, data will first be loaded into the substitute table instead of
     * the target table, when ETL successfully completed,the target will be replaced.
     * This field represents the substitute table.
     */
    private Table substituteTable;

    private long extractChunkSize;

    private long loadChunkSize;

    private boolean clear;

    private ConflictStrategy conflictStrategy;

    private PaginationStrategy paginationStrategy;

    private BatchInsertStrategy batchInsertStrategy;


}
