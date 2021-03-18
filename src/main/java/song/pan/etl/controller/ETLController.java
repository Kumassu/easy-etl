package song.pan.etl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;
import song.pan.etl.common.util.CacheHolder;
import song.pan.etl.common.web.ApiResponse;
import song.pan.etl.controller.vo.ETLConfigVO;
import song.pan.etl.rdbms.ConnectionProperties;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.RdbmsServerFactory;
import song.pan.etl.rdbms.element.Table;
import song.pan.etl.service.ETLService;
import song.pan.etl.service.domain.ETLConfig;
import song.pan.etl.service.domain.ETLTask;

import java.util.concurrent.CompletableFuture;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api")
@Api(tags = "ETL")
public class ETLController {


    @Autowired
    ETLService etlService;


    @PostMapping("/v1/tasks")
    @ApiOperation("Create etl task")
    public ApiResponse createTask(@RequestBody ETLConfigVO vo, @RequestParam(required = false, defaultValue = "true") boolean async) {
        ETLTask task = new ETLTask(map(vo));
        CacheHolder.set(CacheHolder.CacheType.ETL_TASK, task.getId(), task);
        if (async) {
            CompletableFuture.runAsync(() -> etlService.execute(task));
            return ApiResponse.ok(task.getId());
        }
        etlService.execute(task);
        return ApiResponse.ok(task);
    }


    @GetMapping("/v1/tasks/{id}")
    @ApiOperation("Retrieve etl tasks")
    public ApiResponse<ETLTask> getTasks(@PathVariable("id") String id) {
        return ApiResponse.ok((ETLTask) CacheHolder.get(CacheHolder.CacheType.ETL_TASK, id));
    }


    ETLConfig map(ETLConfigVO vo) {
        ETLConfig config = new ETLConfig();

        ETLConfigVO.Source source = vo.getSource();
        Assert.notNull(source, "Source must be specified");

        ETLConfigVO.Destination destination = vo.getDestination();
        Assert.notNull(source, "Destination must be specified");

        ETLConfigVO.RuntimeSetting setting = vo.getSetting();
        if (null == setting) {
            vo.setSetting(new ETLConfigVO.RuntimeSetting());
        }
        BeanUtils.copyProperties(vo.getSetting(), config);

        // source server
        ConnectionProperties srcProp = new ConnectionProperties();
        BeanUtils.copyProperties(source, srcProp);

        RdbmsServer sourceServer = RdbmsServerFactory.getServer(srcProp);
        config.setSourceServer(sourceServer);

        config.setPaginationKeys(source.getPaginationKeys());
        config.setSourceServerPreScripts(source.getPreScripts());
        config.setSourceServerPostScripts(source.getPostScripts());

        config.setQuery(source.getQuery());

        // source table
        Table srcTable = new Table();
        srcTable.setName(source.getTable());
        srcTable.setCatalog(source.getCatalog());
        srcTable.setSchema(source.getSchema());
        srcTable.setColumns(source.getColumns());
        config.setSourceTable(srcTable);

        // destination server
        ConnectionProperties destProp = new ConnectionProperties();
        BeanUtils.copyProperties(destination, destProp);

        RdbmsServer destServer = RdbmsServerFactory.getServer(destProp);
        config.setDestServer(destServer);

        config.setTargetTablePostScripts(destination.getTargetTablePostScripts());
        config.setSourceServerPreScripts(destination.getPreScripts());
        config.setSourceServerPostScripts(destination.getPostScripts());

        // destination table
        Table destTable = new Table();
        destTable.setName(destination.getTable());
        destTable.setCatalog(destination.getCatalog());
        destTable.setSchema(destination.getSchema());
        destTable.setColumns(destination.getColumns());
        config.setDestTable(destTable);

        return config;
    }

}
