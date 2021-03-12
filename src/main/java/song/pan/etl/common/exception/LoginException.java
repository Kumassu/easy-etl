package song.pan.etl.common.exception;

public class LoginException extends GeneralException {

    public LoginException() {
        super(ErrorType.LOGIN_FAIL.getCode());
    }

    public LoginException(String msg) {
        super(ErrorType.LOGIN_FAIL.getCode(), msg);
    }

    public LoginException(Throwable e) {
        super(ErrorType.LOGIN_FAIL.getCode(), e);
    }

}
