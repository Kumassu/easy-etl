package song.pan.etl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.*;
import song.pan.etl.common.util.CacheHolder;
import song.pan.etl.common.web.ApiResponse;
import song.pan.etl.rdbms.ConnectionProperties;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.RdbmsServerFactory;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Commander")
public class SQLCommander {


    @PostMapping("/v1/servers/{server}/queries")
    @ApiOperation("execute query")
    public ApiResponse<Long> execute(@PathVariable("server") String server, @RequestBody String query) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.execute(query));
        } finally {
            rdbmsServer.disconnect();
        }
    }


}
