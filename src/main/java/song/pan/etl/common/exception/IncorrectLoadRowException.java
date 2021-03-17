package song.pan.etl.common.exception;


public class IncorrectLoadRowException extends GeneralException {

    public IncorrectLoadRowException() {
        super(ErrorType.INCORRECT_LOAD_ROW.getCode());
    }

    public IncorrectLoadRowException(String msg) {
        super(ErrorType.INCORRECT_LOAD_ROW.getCode(), msg);
    }

    public IncorrectLoadRowException(Throwable e) {
        super(ErrorType.INCORRECT_LOAD_ROW.getCode(), e);
    }


}
