package song.pan.etl.common.exception;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeneralException extends RuntimeException {

    private int code;

    public GeneralException(int code) {
        this.code = code;
    }

    public GeneralException(int code, String message) {
        super(message);
        this.code = code;
    }

    public GeneralException(int code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    public GeneralException(Throwable cause) {
        super(cause);
    }
}
