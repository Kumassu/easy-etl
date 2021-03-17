package song.pan.etl.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import song.pan.etl.common.util.ConcurrentUtils;
import song.pan.etl.common.util.MapperUtils;
import song.pan.etl.rdbms.DataType;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.element.Column;
import song.pan.etl.rdbms.element.Table;
import song.pan.etl.service.domain.BatchInsertStrategy;
import song.pan.etl.service.domain.ConflictStrategy;
import song.pan.etl.service.domain.ETLConfig;
import song.pan.etl.service.domain.ETLTask;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Service
@Slf4j
public class ETLService {

    ETLWorker getWorker(ETLTask task) {
        return new ETLWorker(task);
    }


    public void execute(ETLTask task) {

        validate(task);

        ETLWorker worker = getWorker(task);

        try {
            worker.doEtl();
        } catch (Exception e) {
            clear(task);
            throw e;
        } finally {
            close(task);
        }
    }


    void validate(ETLTask task) {
        ETLConfig config = task.getConfig();

        setDefaults(config);

        feedSourceTable(config);

        config.getSourceServer().validate(config.getQuery());

        feedDestTable(config);

        Assert.isTrue(StringUtils.hasText(config.getDestTable().getName()), "Destination table name not found");

        log.info("[Validate] ETL config: {}", MapperUtils.prettyJson(config));
    }


    void setDefaults(ETLConfig config) {

        if (null == config.getSourceServerPreScripts()) {
            config.setSourceServerPreScripts(Collections.emptyList());
        }
        if (null == config.getSourceServerPostScripts()) {
            config.setSourceServerPostScripts(Collections.emptyList());
        }
        if (null == config.getDestServerPreScripts()) {
            config.setDestServerPreScripts(Collections.emptyList());
        }
        if (null == config.getDestServerPostScripts()) {
            config.setDestServerPostScripts(Collections.emptyList());
        }
        if (null == config.getTargetTablePostScripts()) {
            config.setTargetTablePostScripts(Collections.emptyList());
        }
        if (null == config.getPaginationKeys()) {
            config.setPaginationKeys(new LinkedList<>());
        }
        if (null == config.getSourceTable().getColumns()) {
            config.getSourceTable().setColumns(new LinkedList<>());
        }
        if (null == config.getDestTable().getColumns()) {
            config.getDestTable().setColumns(new LinkedList<>());
        }
        if (0 == config.getChannel()) {
            config.setChannel(5);
        }
        if (0 == config.getExtractChunkSize()) {
            config.setExtractChunkSize(10000);
        }
        if (0 == config.getLoadChunkSize()) {
            config.setLoadChunkSize(10000);
        }
        if (null == config.getConflictStrategy()) {
            config.setConflictStrategy(ConflictStrategy.DROP);
        }
        if (null == config.getBatchInsertStrategy()) {
            config.setBatchInsertStrategy(BatchInsertStrategy.JDBC);
        }

    }

    void feedSourceTable(ETLConfig config) {
        RdbmsServer sourceServer = config.getSourceServer();
        Table sourceTable = config.getSourceTable();

        // if column not specified
        if (sourceTable.getColumns().isEmpty()) {
            // if query exists, use query to find columns first
            if (StringUtils.hasText(config.getQuery())) {
                sourceTable.setColumns(sourceServer.columnsOf(config.getQuery()));
            } else if (StringUtils.hasText(sourceTable.getName())) {
                sourceTable.setColumns(sourceServer.columnsOf(sourceTable));
                config.setQuery(sourceServer.select(sourceTable));
            } else {
                throw new IllegalArgumentException("table/query not found");
            }
        }
        // if column specified, validate column type
        // if query exists, find all the columns from query
        else if (StringUtils.hasText(config.getQuery())) {
            replaceNoTypeColumns(sourceServer.columnsOf(config.getQuery()), sourceTable.getColumns());
        }
        // find all the columns from table
        else if (StringUtils.hasText(sourceTable.getName())) {
            replaceNoTypeColumns(sourceServer.columnsOf(sourceTable), sourceTable.getColumns());
            config.setQuery(sourceServer.select(sourceTable));
        }
    }


    void feedDestTable(ETLConfig config) {
        List<Column> sourceColumns = config.getSourceTable().getColumns();
        List<Column> destColumns = config.getDestTable().getColumns();

        for (Column srcColumn : sourceColumns) {
            boolean found = false;
            for (Column destColumn : destColumns) {
                if (srcColumn.getName().equals(destColumn.getName())
                        && null == destColumn.getType()) {
                    setColumnType(config.getDestServer(), srcColumn.getTypeIndex(), destColumn);
                    found = true;
                    break;
                }
            }
            if (!found) {
                Column destColumn = new Column();
                BeanUtils.copyProperties(srcColumn, destColumn);
                setColumnType(config.getDestServer(), srcColumn.getTypeIndex(), destColumn);
                destColumns.add(destColumn);
            }
        }
    }

    void setColumnType(RdbmsServer server, int typeIndex, Column column) {
        DataType dataType = server.typeOf(typeIndex);
        if (null == dataType) {
            throw new IllegalArgumentException("Column type not support, name: " + column.getName() +
                    ", type index: " + typeIndex);
        }
        column.setType(dataType.getTypeName());
        column.setTypeIndex(dataType.getTypeIndex());
    }

    private void replaceNoTypeColumns(List<Column> fullFeed, List<Column> columnsToCheck) {
        columnsToCheck.forEach(c -> {
            if (!StringUtils.hasText(c.getType())) {
                fullFeed.forEach(column -> {
                    if (c.getName().equals(column.getName())) {
                        c.setType(column.getType());
                        c.setLength(column.getLength());
                        c.setTypeIndex(column.getTypeIndex());
                    }
                });
            }
        });
    }


    void clear(ETLTask task) {
        ETLConfig config = task.getConfig();
        // clear replica table if exists
        Optional.ofNullable(config.getReplicaTable()).ifPresent(table -> {
            log.info("[Clean] Drop replica table {}", config.getSourceServer().fullQualifiedNameOf(table));
            config.getSourceServer().dropTable(table);
        });

        // clear substitute table and target table
        Optional.ofNullable(config.getSubstituteTable()).ifPresent(table -> {
            log.info("[Clean] Drop substitute table {}", config.getDestServer().fullQualifiedNameOf(table));
            config.getDestServer().dropTable(table);
        });

        if (config.isDropTargetTable()) {
            config.getDestServer().dropTable(config.getDestTable());
        }

    }


    void close(ETLTask task) {
        task.getThreadPools().forEach(pool -> {
            if (!closeThreadPool(pool)) {
                log.warn("[Clean] Can't close resource: {}", pool);
            }
        });
        task.getConfig().getSourceServer().disconnect();
        task.getConfig().getDestServer().disconnect();
    }

    boolean closeThreadPool(ThreadPoolExecutor threadPool) {
        int cnt = 0;
        while (!threadPool.isTerminated()) {
            threadPool.shutdownNow();
            ConcurrentUtils.sleep(TimeUnit.SECONDS, 1);
            log.info("[Clean] Waiting for thread pool terminating: " + ++cnt + "s");
            if (cnt >= 30) {
                return false;
            }
        }
        return true;
    }

}
