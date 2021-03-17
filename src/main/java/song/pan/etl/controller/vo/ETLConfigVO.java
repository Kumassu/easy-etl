package song.pan.etl.controller.vo;

import io.swagger.annotations.ApiModel;
import lombok.Getter;
import lombok.Setter;
import song.pan.etl.rdbms.ConnectionProperties;
import song.pan.etl.rdbms.element.Column;
import song.pan.etl.service.domain.BatchInsertStrategy;
import song.pan.etl.service.domain.ConflictStrategy;
import song.pan.etl.service.domain.PaginationStrategy;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
@ApiModel("ETL Config")
public class ETLConfigVO {


    private Source source;
    private Destination destination;
    private RuntimeSetting setting;


    @Getter
    @Setter
    public static class CommonDatabaseProperties extends ConnectionProperties {
        private String table;
        private String catalog;
        private String schema;
        private List<String> preScripts;
        private List<String> postScripts;
        private List<Column> columns;
    }


    @Getter
    @Setter
    public static class Source extends CommonDatabaseProperties {
        private String query;
        private List<String> paginationKeys;
    }


    @Getter
    @Setter
    public static class Destination extends CommonDatabaseProperties {
        private List<String> targetTablePostScripts;
    }


    @Getter
    @Setter
    public static class RuntimeSetting {
        private int channel;
        private String replicaDatabase;
        private long extractChunkSize;
        private long loadChunkSize;
        private boolean clear;
        private ConflictStrategy conflictStrategy;
        private PaginationStrategy paginationStrategy;
        private BatchInsertStrategy batchInsertStrategy;
    }

}
