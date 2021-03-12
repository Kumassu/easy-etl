package song.pan.etl.common.exception;

public class DisconnectException extends GeneralException {

    public DisconnectException() {
        super(ErrorType.LOGIN_FAIL.getCode());
    }

    public DisconnectException(String msg) {
        super(ErrorType.LOGIN_FAIL.getCode(), msg);
    }

    public DisconnectException(Throwable e) {
        super(ErrorType.LOGIN_FAIL.getCode(), e);
    }

}
