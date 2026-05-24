package org.swm.kafkapractice.api;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.swm.kafkapractice.event.OrderEvent;
import org.swm.kafkapractice.producer.OrderProducer;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer producer;

    @PostMapping
    public ResponseEntity<String> publish(@RequestBody OrderRequest request) {
        try {
            OrderEvent event = new OrderEvent(request.orderId(), request.status(), request.amount());
            producer.send(event);
            return ResponseEntity.accepted().body("queued: " + request.orderId());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }

    public record OrderRequest(String orderId, String status, long amount) {}
}
