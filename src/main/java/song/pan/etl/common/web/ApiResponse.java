package song.pan.etl.common.web;

import lombok.Getter;
import lombok.Setter;
import song.pan.etl.common.exception.ErrorType;

@Getter
@Setter
public class ApiResponse<T> {
    private Integer code;
    private String msg;
    private T data;

    public ApiResponse() { }

    public ApiResponse(int code, String msg, T data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    public static <T> ApiResponse<T> ok() {
        return new ApiResponse(200, null, null);
    }

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse(200, null, data);
    }

    public static <T> ApiResponse<T> fail(ErrorType type, String msg) {
        return new ApiResponse(type.getCode(), msg, (Object)null);
    }

}