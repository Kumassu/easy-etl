package song.pan.etl.service;

import lombok.extern.slf4j.Slf4j;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.element.Table;
import song.pan.etl.service.domain.ETLConfig;
import song.pan.etl.service.domain.ETLStatus;
import song.pan.etl.service.domain.ETLTask;

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

    }

    void etl() {

    }

    void postEtl() {

    }


}
