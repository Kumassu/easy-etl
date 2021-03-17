package song.pan.etl.common.exception;


public class InvalidPaginationKeyException extends GeneralException {

    public InvalidPaginationKeyException() {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode());
    }

    public InvalidPaginationKeyException(String msg) {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode(), msg);
    }

    public InvalidPaginationKeyException(Throwable e) {
        super(ErrorType.ILLEGAL_ARGUMENT.getCode(), e);
    }

}
