package song.pan.etl.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StringUtils;
import song.pan.etl.common.exception.*;
import song.pan.etl.common.exception.IllegalArgumentException;
import song.pan.etl.common.util.ConcurrentUtils;
import song.pan.etl.config.AppSetting;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.element.*;
import song.pan.etl.service.domain.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Slf4j
public class ETLWorker {

    private ETLTask task;
    private ETLConfig config;
    private ETLStatus status;
    private RdbmsServer sourceServer;
    private RdbmsServer destServer;
    private Table sourceTable;
    private Table destTable;


    public ETLWorker(ETLTask task) {
        this.task = task;
        this.config = task.getConfig();
        this.status = task.getStatus();
        this.sourceServer = task.getConfig().getSourceServer();
        this.destServer = task.getConfig().getDestServer();
        this.sourceTable = task.getConfig().getSourceTable();
        this.destTable = task.getConfig().getDestTable();
    }


    public void doEtl() {
        preEtl();
        etl();
        postEtl();
    }


    void preEtl() {

        // pre-scripts in source server
        config.getSourceServerPostScripts().forEach(s -> sourceServer.execute(s));


        // post-scripts in destination server
        config.getDestServerPreScripts().forEach(s -> destServer.execute(s));

        createTargetTable();

        // post-scripts in destination server after table creation
        config.getTargetTablePostScripts().forEach(s -> destServer.execute(s));
    }


    void createTargetTable() {

        // create a new table if not exist
        if (!destServer.isTableExist(destTable)) {
            destServer.createTable(destTable);
            config.setDropTargetTable(true);
            log.info("[PreETL] {}", destServer.fullQualifiedNameOf(destTable));
            return;
        }

        // conflict
        ConflictStrategy strategy = config.getConflictStrategy();
        log.info("[PreETL] Table {} exists, conflict strategy: {}", destServer.fullQualifiedNameOf(destTable), strategy);
        assert strategy != null;
        switch (strategy) {
            case DROP:
            case TRUNCATE:
            case CONTINUE:
                String substituteTableName = destTable.getName() + AppSetting.SUBSTITUTE_SUFFIX;
                Table substituteTable = new Table(destTable.getCatalog(), destTable.getSchema(), substituteTableName);
                if (destServer.isTableExist(substituteTable)) {
                    log.info("[PreETL] Table {} exists, drop it", destServer.fullQualifiedNameOf(substituteTable));
                    destServer.dropTable(substituteTable);
                }
                substituteTable.setColumns(new LinkedList<>(destTable.getColumns()));
                destServer.createTable(substituteTable);
                log.info("[PreETL] Table {} created", destServer.fullQualifiedNameOf(substituteTable));
                config.setSubstituteTable(substituteTable);
                break;
            case EXIT:
                throw new TableConflictException("Table already exists: " + destServer.fullQualifiedNameOf(destTable));
            default:
        }
    }

    void etl() {

        if (totalWorkload() <= config.getExtractChunkSize()) {
            nonPaging();
            return;
        }

        setPaginationKey();


        PaginationStrategy strategy = config.getPaginationStrategy();
        log.info("[ETL] Pagination strategy: {}", strategy);
        assert strategy != null;
        switch (strategy) {
            case GENERATE:
                generate();
                break;
            case DEPENDENT:
                dependent();
                break;
            case DISTRIBUTE:
                distribute();
                break;
            case OFFSET_LIMIT:
                offsetLimit();
                break;
            default:
                cursor();
        }

        log.info("[ETL] complete");
    }


    long totalWorkload() {
        log.info("[ETL] Count workload...");
        long workload = sourceServer.count(config.getQuery());
        status.setExpect(workload);
        log.info("[ETL] Workload: {}", workload);
        return workload;
    }


    void setPaginationKey() {
        PaginationStrategy strategy = config.getPaginationStrategy();

        // these three strategies do not need  pagination key
        if (strategy == PaginationStrategy.CURSOR
                || strategy == PaginationStrategy.GENERATE
                || strategy == PaginationStrategy.OFFSET_LIMIT) {
            return;
        }

        // try find suitable pagination key
        if (strategy == null || config.getPaginationKeys().isEmpty()) {
            findPaginationKey();
        }

        // use cursor if find no pagination key
        if (config.getPaginationKeys().isEmpty()) {
            if (strategy != null) {
                throw new NoPaginationKeyException("No candidate pagination key found for strategy: " + strategy);
            }
            config.setPaginationStrategy(PaginationStrategy.CURSOR);
            return;
        }

        // use distribute as default if not specified
        if (strategy == null) {
            config.setPaginationStrategy(PaginationStrategy.DISTRIBUTE);
        }
    }


    void findPaginationKey() {

        if (!StringUtils.hasText(sourceTable.getName())) {
            return;
        }

        List<String> paginationKeys = config.getPaginationKeys();

        if (paginationKeys.isEmpty()) {
            log.info("[ETL] Pagination key not specified, try finding one from: {}", sourceServer.fullQualifiedNameOf(sourceTable));

            // find primary key
            List<String> primaryKeys = sourceServer.primaryKeyOf(sourceTable);
            if (primaryKeys.isEmpty()) {
                log.info("[ETL] Found primary key: {}", primaryKeys);
                paginationKeys = primaryKeys;
            }
            // find index
            else {
                List<Index> indices = sourceServer.indexesOf(sourceTable);
                if (indices.isEmpty()) {
                    return;
                }
                Optional<Index> unique = indices.stream().filter(Index::isUnique).findAny();
                if (unique.isPresent()) {
                    paginationKeys = unique.get().getColumns();
                    log.info("[ETL] Found unique index: {}", paginationKeys);
                } else {
                    paginationKeys = indices.get(0).getColumns();
                    log.info("[ETL] Found index: {}", paginationKeys);
                }
            }
        }

        // ensure query results contain all pagination keys
        for (String key : paginationKeys) {
            if (sourceTable.getColumns().stream().noneMatch(column -> column.getName().equals(key))) {
                log.info("[ETL] Query results not contain column: {}", key);
                return;
            }
        }
        config.setPaginationKeys(paginationKeys);
    }


    void generate() {
        ThreadPoolExecutor threadPool = newThreadPool();

        long pageNumber = estimatePageNumber(status.getExpect(), config.getExtractChunkSize());
        log.info("[ETL] Page number: {}", pageNumber);

        // create replica table
        String replica = !StringUtils.hasText(sourceTable.getName()) ? AppSetting.REPLICA_PREFIX + System.currentTimeMillis() :
                AppSetting.REPLICA_PREFIX + sourceTable.getName();
        if (!StringUtils.hasText(config.getReplicaDatabase())) {
            config.setReplicaDatabase(sourceTable.getCatalog());
        }
        Table replicaTable = new Table(config.getReplicaDatabase(), replica, new LinkedList<>(sourceServer.columnsOf(config.getQuery())));
        sourceServer.copyToGenerateId(config.getQuery(), replicaTable, AppSetting.GENERATE_ID);
        Column id = new Column(AppSetting.GENERATE_ID);
        replicaTable.getColumns().add(id);
        config.setReplicaTable(replicaTable);

        String selectAll = sourceServer.select(replicaTable);
        List<Future> futures = new LinkedList<>();
        for (long pageIndex = 1; pageIndex <= pageNumber; pageIndex++) {
            long start = (pageIndex - 1) * config.getExtractChunkSize() + 1;
            long end = pageIndex * config.getExtractChunkSize();
            Page page = new Page(pageIndex);
            futures.add(threadPool.submit(() -> {
                long begin = System.currentTimeMillis();
                page.setData(sourceServer.query(selectAll, id, start, end));
                page.setExtractTimeMs(System.currentTimeMillis() - begin);
                getLoader().load(page);
            }));
        }

        ConcurrentUtils.wait(futures);

        checkLoadedRows();
    }


    ThreadPoolExecutor newThreadPool() {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                task.getConfig().getChannel(),
                task.getConfig().getChannel(),
                0,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>());
        task.getThreadPools().add(threadPool);
        return threadPool;
    }


    long estimatePageNumber(long total, long pageSize) {
        return (total % pageSize == 0) ? total / pageSize : (total / pageSize + 1);
    }


    @FunctionalInterface
    interface Loader {
        long load(Page page);
    }


    protected Loader getLoader() {
        return (page) -> {
            List<Row> rows = page.getData();

            Table to = Optional.ofNullable(config.getSubstituteTable()).orElse(destTable);

            long cnt = 0;
            long begin = System.currentTimeMillis();
            try {
                long chunkSize = config.getLoadChunkSize();
                // split to multiple small pages
                if (rows.size() > chunkSize) {
                    for (long i = 0; i < rows.size(); i++) {
                        long end = Math.min(i + chunkSize, rows.size());
                        Page childPage = new Page(rows.subList((int) i, (int) end));
                        long loadRows = loadPage(destServer, to, childPage);
                        cnt += loadRows;
                        i = end;
                    }
                } else {
                    cnt = loadPage(destServer, to, page);
                }
            } catch (Exception e) {
                status.getErrors().add(e);
                throw new SystemException(e);
            }
            page.setLoadTimeMs(System.currentTimeMillis() - begin);

            synchronized (ETLWorker.class) {
                long total = status.getCurrent() + cnt;
                if (total == status.getExpect()) {
                    status.setSuccess(Boolean.TRUE);
                }
                status.setCurrent(total);
            }

            log.info("[ETL] Page {} [{}%], current: {}, extract:{} ms, load: {}ms",
                    page.getIndex(),
                    String.format("%.2f", status.getCurrent() * 100.00 / status.getExpect()),
                    status.getCurrent(),
                    page.getExtractTimeMs(),
                    page.getLoadTimeMs());
            return cnt;
        };
    }


    void checkLoadedRows() {
        if (status.getExpect() != status.getCurrent()) {
            config.setClear(true);
            throw new IncorrectLoadRowException("Expect: " + status.getExpect() + ", loaded: " + status.getCurrent());
        }
    }


    void dependent() {
        List<String> paginationKeys = config.getPaginationKeys();

        String majorKey = paginationKeys.get(0);
        log.info("[ETL] Major pagination key: {}", majorKey);

        Row lastRow = sourceServer.lastRowOf(config.getQuery(), paginationKeys);
        log.info("[ETL] Last row: {}", lastRow);
        Object endBoundary = lastRow.getColumn(majorKey);
        log.info("[ETL] End boundary: {}", endBoundary);
        assert null != endBoundary;

        ThreadPoolExecutor threadPool = newThreadPool();
        long chunkSize = config.getExtractChunkSize();
        Object startBoundary = null;
        long pageIndex = 0;
        List<Future> futures = new LinkedList<>();
        boolean queryComplete = false;
        while (!queryComplete) {
            Object preStartBoundary = startBoundary;
            String query = addBoundary(sourceServer, config.getQuery(), majorKey, startBoundary, endBoundary);
            query = query + " ORDER BY " + String.join(",", paginationKeys);
            long begin = System.currentTimeMillis();
            List<Row> rows = sourceServer.topRowsOf(query, chunkSize);
            Page page = new Page(++pageIndex, rows);
            page.setExtractTimeMs(System.currentTimeMillis() - begin);

            // refresh start boundary
            startBoundary = rows.get(rows.size() - 1).getColumn(majorKey);

            // end boundary not reach yet,
            // remove all the rows having the end boundary
            if (!Objects.equals(lastRow.getColumn(majorKey), startBoundary)) {
                Object sb = startBoundary;
                List<Row> copy = rows.stream().filter(row -> !Objects.equals(row.getColumn(majorKey), sb)).collect(Collectors.toList());
                if (copy.isEmpty()) {
                    log.info("[ETL] Page[{}] has a same pagination key: {}={}, current size: {}, will double it to query again ",
                            page.getIndex(), majorKey, startBoundary, chunkSize);
                    startBoundary = preStartBoundary;
                    chunkSize = chunkSize * 2;
                    pageIndex--;
                    continue;
                }
                page.setData(copy);
            }
            // end boundary reached
            else {
                log.info("[ETL] Found max value of [{}], current page: [{}, {}], last row: {}",
                        majorKey, preStartBoundary, startBoundary, lastRow);
                boolean reachEnd = false;
                int lastRowIndex = 0;
                for (int i = 0; i < rows.size(); i++) {
                    boolean allColumnMatch = true;
                    for (Map.Entry<String, Object> entry : lastRow.entrySet()) {
                        if (!Objects.equals(entry.getValue(), rows.get(i).getColumn(entry.getKey()))) {
                            allColumnMatch = false;
                            break;
                        }
                    }
                    if (allColumnMatch) {
                        reachEnd = true;
                        lastRowIndex = i;
                    }
                }
                if (reachEnd) {
                    log.info("[ETL] Query complete");
                    page.setData(rows.subList(0, lastRowIndex + 1));
                    queryComplete = true;
                } else {
                    log.info("[ETL] Max major key shows,{}={}, but has not reached the end, last row: {}, " +
                            "will double page size to query again", majorKey, endBoundary, lastRow);
                    startBoundary = preStartBoundary;
                    chunkSize = chunkSize * 2;
                    pageIndex--;
                    continue;
                }
            }
            Optional.ofNullable(page).ifPresent(p -> futures.add(threadPool.submit(() -> getLoader().load(p))));

            // rollback extract chunk size
            chunkSize = config.getExtractChunkSize();
        }

        ConcurrentUtils.wait(futures);

        checkLoadedRows();
    }


    /**
     * Hard-coded, may not suit for every situation
     */
    private String addBoundary(RdbmsServer server, String query, String key, Object start, Object end) {
        String condition = "";
        if (start != null) {
            condition = key + " >= " + server.format(start);
            if (end != null) {
                condition = condition + " AND " + key + " <= " + server.format(end);
            }
        } else if (end != null) {
            condition = key + " <= " + server.format(end);
        }

        SQLResolver.Select select = SQLResolver.resolveSelect(query);
        if (null != select.getWhereItem()) {
            select.getWhereItem().getMembers().add(0, condition);
        } else {
            SQLResolver.Item where = new SQLResolver.Item(SQLResolver.ItemType.WHERE);
            where.setMembers(Collections.singletonList(condition));
            select.setWhereItem(where);
        }
        return select.toString();
    }


    void distribute() {
        ThreadPoolExecutor threadPool = newThreadPool();

        String majorKey = config.getPaginationKeys().get(0);

        Column majorColumn = sourceTable.getColumns().stream().filter(c -> c.getName().equals(majorKey)).findAny()
                .orElseThrow(() -> new IllegalArgumentException("Pagination key [" + majorKey + "] not found in query results"));


        long min = Long.parseLong(sourceServer.minOf(config.getQuery(), majorKey).toString());
        long max = Long.parseLong(sourceServer.maxOf(config.getQuery(), majorKey).toString());

        log.info("[ETL] Find boundary of major key [{}], [{}, {}]", majorKey, min, max);

        long range = max - min + 1;

        long pageNumber = estimatePageNumber(status.getExpect(), config.getExtractChunkSize());
        long step = Math.max(estimatePageNumber(range, pageNumber), 1);
        log.info("[ETL] Page number: {}, step: {}", pageNumber, step);

        long start = min;
        long end = min + step - 1;
        long pageIndex = 0;
        List<Future> futures = new LinkedList<>();
        for (;;) {
            Page page = new Page(++pageIndex);
            long finalStart = start;
            long finalEnd = end;
            futures.add(threadPool.submit(() -> {
                long b = System.currentTimeMillis();
                page.setData(sourceServer.query(config.getQuery(), majorColumn, finalStart, finalEnd));
                page.setExtractTimeMs(System.currentTimeMillis() - b);
                getLoader().load(page);
            }));
            log.info("[ETL] Page {} : {} -> [{}, {}]", pageIndex, majorKey, start, end);
            if (end == max) {
                break;
            }
            start = end + 1;
            end = Math.min(end + step, max);
        }

        ConcurrentUtils.wait(futures);

        checkLoadedRows();
    }

    void offsetLimit() {
        long pageNumber = estimatePageNumber(status.getExpect(), config.getExtractChunkSize());
        log.info("[ETL] Page number: {}", pageNumber);

        ThreadPoolExecutor threadPool = newThreadPool();

        List<Future> futures = new LinkedList<>();
        for (long pageIndex = 1; pageIndex <= pageNumber; pageIndex++) {
            long offset = (pageIndex - 1) * config.getExtractChunkSize();
            Page page = new Page(pageIndex);
            futures.add(threadPool.submit(() -> {
                long begin = System.currentTimeMillis();
                page.setData(sourceServer.query(config.getQuery(), offset, config.getExtractChunkSize()));
                page.setExtractTimeMs(System.currentTimeMillis() - begin);
                getLoader().load(page);
            }));
        }

        ConcurrentUtils.wait(futures);

        checkLoadedRows();
    }


    void cursor() {
        long pageNumber = estimatePageNumber(status.getExpect(), config.getExtractChunkSize());
        log.info("[ETL] Page number: {}", pageNumber);

        ThreadPoolExecutor threadPool = newThreadPool();

        int pageIndex = 0;
        List<Future> futures = new LinkedList<>();
        try (Connection connection = sourceServer.getDataSource().getConnection();
             ResultSet resultSet = connection.createStatement().executeQuery(config.getQuery()))  {
            resultSet.setFetchSize((int) config.getExtractChunkSize());
            List<Row> rows = new LinkedList<>();
            long begin = System.currentTimeMillis();
            long rowNum = 0;
            while (resultSet.next()) {
                Row row = new Row();
                for (Column column : sourceTable.getColumns()) {
                    row.setColumn(column.getName(), resultSet.getObject(column.getName()));
                }
                rows.add(row);
                rowNum++;
                if (rows.size() == config.getExtractChunkSize() || rowNum == status.getExpect()) {
                    Page page = new Page(rows);
                    page.setIndex(++pageIndex);
                    page.setExtractTimeMs(System.currentTimeMillis() - begin);
                    futures.add(threadPool.submit(() -> getLoader().load(page)));
                    begin = System.currentTimeMillis();
                    rows = new LinkedList<>();
                }
            }
        } catch (SQLException e) {
            throw new SystemException(e);
        }

        ConcurrentUtils.wait(futures);

        checkLoadedRows();
    }


    void nonPaging() {
        long begin = System.currentTimeMillis();
        List<Row> rows = sourceServer.query(config.getQuery());
        Page page = new Page(1, rows);
        page.setExtractTimeMs(System.currentTimeMillis() - begin);
        getLoader().load(page);
    }


    void postEtl() {
        Optional.ofNullable(config.getSubstituteTable()).ifPresent(table -> {
            ConflictStrategy strategy = config.getConflictStrategy();
            log.info("[PostETL] Conflict strategy: {}", strategy);

            String destTableName = destServer.fullQualifiedNameOf(destTable);
            String substituteTableName = destServer.fullQualifiedNameOf(table);

            assert null != strategy;
            switch (strategy) {
                case DROP:
                    log.info("[PostETL] Drop old table: {}", destTableName);
                    destServer.dropTable(destTable);
                    log.info("[PostETL] Rename substitute table {} to {}", substituteTableName, destTableName);
                    destServer.renameTable(table, destTable.getName());
                    break;
                case TRUNCATE:
                    log.info("[PostETL] Truncate old table: {}", destTableName);
                    destServer.truncateTable(destTable);
                case CONTINUE:
                    log.info("[PostETL] Copy data from {} to {}", substituteTableName, destTableName);
                    destServer.copyTable(table, destTable);
                    log.info("[PostETL] Drop substitute table {}", substituteTableName);
                    destServer.dropTable(table);
                    break;
                default:
            }
        });

        // post scripts
        config.getSourceServerPostScripts().forEach(s -> sourceServer.execute(s));
        config.getDestServerPostScripts().forEach(s -> destServer.execute(s));
    }


    long loadPage(RdbmsServer server, Table to, Page page) {
        if (null == page.getData() || page.getData().isEmpty()) {
            return 0;
        }

        BatchInsertStrategy strategy = config.getBatchInsertStrategy();
        assert strategy != null;

        switch (strategy) {
            case JDBC:
                return jdbcBatchInsert(server, to, page);
            case SPRING:
                return springBatchInsert(server, to, page);
            default:
                throw new UnsupportedOperationException();
        }
    }


    long springBatchInsert(RdbmsServer server, Table to, Page page) {
        List<Row> rows = page.getData();

        Map<String, Object>[] data = new HashMap[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<String, Object> entry : rows.get(i).entrySet()) {
                map.put(entry.getKey(), entry.getValue());
            }
            data[i] = map;
        }

        String stmt = server.prepareNamedInsertStatement(to);

        int[] updatedRows = new NamedParameterJdbcTemplate(server.getJdbcTemplate()).batchUpdate(stmt, data);
        return Arrays.stream(updatedRows).summaryStatistics().getSum();
    }



    private void prepareStatement(List<Column> columns, PreparedStatement statement, List<Row> rows) throws SQLException {
        for (Row row : rows) {
            int parameterIndex = 1;
            for (Column column : columns) {
                Object value = row.getColumn(column.getName());
                if (null == value) {
                    statement.setNull(parameterIndex++, column.getTypeIndex());
                } else {
                    statement.setObject(parameterIndex++, value);
                }
            }
            statement.addBatch();
        }
    }



    long jdbcBatchInsert(RdbmsServer server, Table to, Page page) {
        List<Row> rows = page.getData();
        try (Connection connection = server.getDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(server.prepareInsertStatement(to))) {
            connection.setAutoCommit(false);
            prepareStatement(to.getColumns(), statement, rows);
            int[] updatedRows = statement.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            return updatedRows.length;
        } catch (SQLException e) {
            throw new SystemException(e);
        }
    }




}
