package song.pan.etl.common.exception;


public class IllegalArgumentException extends GeneralException {

    public IllegalArgumentException() {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode());
    }

    public IllegalArgumentException(String msg) {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode(), msg);
    }

    public IllegalArgumentException(Throwable e) {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode(), e);
    }

}
