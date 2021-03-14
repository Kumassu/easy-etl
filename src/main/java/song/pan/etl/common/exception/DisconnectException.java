package song.pan.etl.common.exception;

public class DisconnectException extends GeneralException {

    public DisconnectException() {
        super(ErrorType.DISCONNECT_FAIL.getCode());
    }

    public DisconnectException(String msg) {
        super(ErrorType.DISCONNECT_FAIL.getCode(), msg);
    }

    public DisconnectException(Throwable e) {
        super(ErrorType.DISCONNECT_FAIL.getCode(), e);
    }

}
