package song.pan.etl.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class ETLTask {

    private ETLConfig config;
    private ETLStatus status;

    @JsonIgnore
    private List<ThreadPoolExecutor> threadPools;

    public ETLTask(ETLConfig config) {
        this.config = config;
        this.status = new ETLStatus();
        this.threadPools = new LinkedList<>();
    }
}
