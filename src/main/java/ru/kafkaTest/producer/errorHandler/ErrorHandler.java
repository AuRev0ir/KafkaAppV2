package ru.kafkaTest.producer.errorHandler;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import ru.kafkaTest.producer.exception.PartitionException;

@ControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(PartitionException.class)
    public ResponseEntity<String> handlerPartition(PartitionException ex){
        return ResponseEntity.badRequest().body("Not found partition");
    }
}
