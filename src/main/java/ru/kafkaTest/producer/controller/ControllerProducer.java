package ru.kafkaTest.producer.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.kafkaTest.producer.service.ServiceProducer;


@RestController
public class ControllerProducer {

   private final ServiceProducer service;

   @Autowired
    public ControllerProducer(ServiceProducer service) {
        this.service = service;
    }

    @PostMapping("/oneMsg")
    public void sendingOneMessage (@RequestParam Long msgId,
                                   @RequestParam String msg){
        service.sendingOneMessage(msgId, msg);
    }

    @PostMapping("/newsletterMsg")
    public void newsletterMsg (@RequestParam Long quantity,
                               @RequestParam String msg){
        service.newsletterMsg(quantity, msg);

    }

    @PostMapping("/newsletterMsg/byPartition")
    public void newsletterMsgByPartition (@RequestParam Integer partition,
                               @RequestParam Long quantity,
                               @RequestParam String msg){
       service.newsletterMsgByPartition(partition, quantity, msg);
    }

    @PostMapping("/oneMsg/ByPartition")
    public void sendingOneMessage (@RequestParam Long msgId,
                                   @RequestParam String msg,
                                   @RequestParam Integer partition){
        service.sendingOneMessageByPartition(partition,msgId,msg);
    }
}
