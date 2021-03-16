package song.pan.etl.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import song.pan.etl.controller.vo.ETLConfigVO;
import song.pan.etl.controller.vo.ETLTaskVO;
import song.pan.etl.service.ETLService;
import song.pan.etl.service.domain.ETLConfig;
import song.pan.etl.service.domain.ETLTask;

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

    @PostMapping("/v1/etl")
    @ApiOperation("do etl")
    public ETLTaskVO etl(@RequestBody ETLConfigVO vo) {

        ETLTask task = new ETLTask(map(vo));

        etlService.execute(task);

        return map(task);
    }


    ETLConfig map(ETLConfigVO vo) {
        ETLConfig config = new ETLConfig();
        return config;
    }

    ETLTaskVO map(ETLTask vo) {
        ETLTaskVO taskVO = new ETLTaskVO();
        return taskVO;
    }





}
