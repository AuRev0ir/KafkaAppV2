package ru.kafkaTest.producer.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.kafkaTest.producer.service.ServiceProducer;


@RestController
@RequestMapping("api/producerAndConsumer")
public class ControllerProducer {

   private final ServiceProducer service;

   @Autowired
    public ControllerProducer(ServiceProducer service) {
        this.service = service;
    }

    @PostMapping("/oneMessage")
    public ResponseEntity<String> sendOneMessage (@RequestParam Long msgId,
                                                  @RequestParam String msg){
        return ResponseEntity.ok(service.sendOneMessage(msgId, msg));
    }

    @PostMapping("/manyMessages")
    public ResponseEntity<String> sendManyMessages (@RequestParam Long quantity,
                                                    @RequestParam String msg){
        return ResponseEntity.ok(service.sendManyMessages(quantity, msg));

    }

    @PostMapping("/manyMessagesByPartition")
    public ResponseEntity<String> sendManyMessagesByPartition (@RequestParam Integer partition,
                                                               @RequestParam Long quantity,
                                                               @RequestParam String msg){
       return ResponseEntity.ok(service.sendManyMessagesByPartition(partition, quantity, msg));
    }

    @PostMapping("oneMessageByPartition")
    public ResponseEntity<String> sendOneMessageByPartition (@RequestParam Long msgId,
                                                             @RequestParam String msg,
                                                             @RequestParam Integer partition){
        return ResponseEntity.ok(service.sendOneMessageByPartition(partition,msgId,msg));
    }
}
