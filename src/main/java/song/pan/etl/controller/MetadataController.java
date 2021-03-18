package song.pan.etl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import song.pan.etl.common.util.CacheHolder;
import song.pan.etl.common.web.ApiResponse;
import song.pan.etl.rdbms.ConnectionProperties;
import song.pan.etl.rdbms.RdbmsServer;
import song.pan.etl.rdbms.RdbmsServerFactory;
import song.pan.etl.rdbms.element.Column;
import song.pan.etl.rdbms.element.Index;
import song.pan.etl.rdbms.element.Table;

import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@RestController
@RequestMapping("/api")
@Api(tags = "Metadata")
public class MetadataController {


    @GetMapping("/v1/servers/{server}")
    @ApiOperation("Retrieve server")
    public ApiResponse<ConnectionProperties> getServer(@PathVariable("server") String server) {
        return ApiResponse.ok((ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server));
    }


    @GetMapping("/v1/servers")
    @ApiOperation("Retrieve servers")
    public ApiResponse<List<Object>> getServers() {
        return ApiResponse.ok(CacheHolder.get(CacheHolder.CacheType.SERVER));
    }


    @PostMapping("/v1/servers")
    @ApiOperation("Create server")
    public ApiResponse<ConnectionProperties> createServer(@RequestBody ConnectionProperties properties) {
        Assert.notNull(properties, "properties must not be null");
        Assert.hasText(properties.getName(), "server name must be specified");
        Assert.isNull(CacheHolder.get(CacheHolder.CacheType.SERVER, properties.getName()), "server already exists");

        CacheHolder.set(CacheHolder.CacheType.SERVER, properties.getName(), properties);
        return ApiResponse.ok(properties);
    }


    @PutMapping("/v1/servers")
    @ApiOperation("Update server")
    public ApiResponse<ConnectionProperties> updateServer(@RequestBody ConnectionProperties properties) {
        Assert.notNull(properties, "properties must not be null");
        Assert.hasText(properties.getName(), "server name must be specified");
        Assert.notNull(CacheHolder.get(CacheHolder.CacheType.SERVER, properties.getName()), "server not found");
        CacheHolder.set(CacheHolder.CacheType.SERVER, properties.getName(), properties);
        return ApiResponse.ok(properties);
    }


    @DeleteMapping("/v1/servers/{server}")
    @ApiOperation("Remove server")
    public ApiResponse<ConnectionProperties> removeServer(@PathVariable("server") String server) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        CacheHolder.remove(CacheHolder.CacheType.SERVER, server);
        return ApiResponse.ok(properties);
    }


    @GetMapping("/v1/servers/{server}/catalogs")
    @ApiOperation("Retrieve catalogs")
    public ApiResponse<List<String>> catalogsOf(@PathVariable("server") String server) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.catalogs());
        } finally {
            rdbmsServer.disconnect();
        }

    }

    @GetMapping("/v1/servers/{server}/catalogs/{catalog}/tables")
    @ApiOperation("Retrieve catalog")
    public ApiResponse<List<String>> tablesOf(@PathVariable("server") String server, @PathVariable("catalog") String catalog) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.tablesOf(catalog));
        } finally {
            rdbmsServer.disconnect();
        }
    }


    @GetMapping("/v1/servers/{server}/catalogs/{catalog}/tables/{table}/columns")
    @ApiOperation("Retrieve columns")
    public ApiResponse<List<Column>> columnsOf(@PathVariable("server") String server, @PathVariable("catalog") String catalog, @PathVariable("table") String table) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.columnsOf(new Table(catalog, table)));
        } finally {
            rdbmsServer.disconnect();
        }
    }

    @GetMapping("/v1/servers/{server}/catalogs/{catalog}/tables/{table}/columns/{column}")
    @ApiOperation("Retrieve column")
    public ApiResponse<Column> columnOf(@PathVariable("server") String server, @PathVariable("catalog") String catalog,
                                              @PathVariable("table") String table, @PathVariable("column") String column) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.columnsOf(new Table(catalog, table))
                    .stream().filter(c -> c.getName().equals(column)).findAny().orElse(null));
        } finally {
            rdbmsServer.disconnect();
        }
    }


    @GetMapping("/v1/servers/{server}/catalogs/{catalog}/tables/{table}/indexes")
    @ApiOperation("Retrieve indexes")
    public ApiResponse<List<Index>> indexesOf(@PathVariable("server") String server, @PathVariable("catalog") String catalog, @PathVariable("table") String table) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.indexesOf(new Table(catalog, table)));
        } finally {
            rdbmsServer.disconnect();
        }
    }


    @GetMapping("/v1/servers/{server}/catalogs/{catalog}/tables/{table}/indexes/{index}")
    @ApiOperation("Retrieve index")
    public ApiResponse<Index> indexOf(@PathVariable("server") String server, @PathVariable("catalog") String catalog,
                                      @PathVariable("table") String table, @PathVariable("table") String index) {
        ConnectionProperties properties = (ConnectionProperties) CacheHolder.get(CacheHolder.CacheType.SERVER, server);
        Assert.notNull(properties, "server not found: " + server);
        RdbmsServer rdbmsServer = RdbmsServerFactory.getServer(properties);
        try {
            return ApiResponse.ok(rdbmsServer.indexesOf(new Table(catalog, table))
                    .stream().filter(c -> c.getName().equals(index)).findAny().orElse(null));
        } finally {
            rdbmsServer.disconnect();
        }
    }


}
