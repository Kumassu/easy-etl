package song.pan.etl.common.web;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import song.pan.etl.common.exception.ErrorType;
import song.pan.etl.common.exception.GeneralException;

@ControllerAdvice
@Slf4j
public class ErrorHandler {


    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public ApiResponse handle(Exception e) {
        ApiResponse result = new ApiResponse();
        if (e instanceof GeneralException) {
            if (log.isDebugEnabled()) {
//                log.error("{} : {}", e.getClass().getName(), e.getMessage());
                log.error(e.getClass().getName(), e);
            } else {
                log.error(e.getClass().getName(), e);
            }
            result.setCode(((GeneralException) e).getCode());
        } else {
            log.error("System Error", e);
            result.setCode(ErrorType.SYSTEM_ERROR.getCode());
        }
        result.setMsg(e.getClass().getName() + " : " + e.getMessage());
        return result;
    }
}
