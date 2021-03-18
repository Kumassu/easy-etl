package song.pan.etl.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Getter
@Setter
public class ETLStatus {

    private long expect;

    private long current;

    private Boolean success;

    private Date start;
    private Date end;

    @JsonIgnore
    private List<Throwable> errors;

    public ETLStatus() {
        this.start = new Date();
        this.errors = new LinkedList<>();
    }

}
