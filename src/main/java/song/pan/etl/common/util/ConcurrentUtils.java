package song.pan.etl.common.util;

import song.pan.etl.common.exception.SystemException;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Song Pan
 * @version 1.0.0
 */
public class ConcurrentUtils {


    public static void sleep(TimeUnit unit, long time) {
        try {
            unit.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T wait(Future<T> f) {
        try {
            return f.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new SystemException(e);
        }
        return null;
    }

    public static void wait(List<Future> futures) {
        for (Future future : futures) {
            wait(future);
        }
    }


}
