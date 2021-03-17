package song.pan.etl.controller.vo;

import io.swagger.annotations.ApiModel;
import lombok.Getter;
import lombok.Setter;
import song.pan.etl.service.domain.ETLConfig;
import song.pan.etl.service.domain.ETLStatus;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@ApiModel("ETL Task")
@Getter
@Setter
public class ETLTaskVO {

    private ETLConfig config;
    private ETLStatus status;

}
