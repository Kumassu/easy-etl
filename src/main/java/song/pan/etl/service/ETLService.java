package song.pan.etl.service;

import org.springframework.stereotype.Service;
import song.pan.etl.service.domain.ETLTask;

/**
 * @author Song Pan
 * @version 1.0.0
 */
@Service
public class ETLService {

    ETLWorker getWorker(ETLTask task) {
        return new ETLWorker(task);
    }


    public void execute(ETLTask task) {

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

    }


    void clear(ETLTask task) {

    }

    void close(ETLTask task) {

    }

}
