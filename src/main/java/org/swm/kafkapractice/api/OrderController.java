package org.swm.kafkapractice.api;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.swm.kafkapractice.event.OrderEvent;
import org.swm.kafkapractice.outbox.OrderOutboxService;
import org.swm.kafkapractice.producer.OrderProducer;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer producer;
    private final OrderOutboxService outboxService;

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

    /**
     * 동기 발행 — broker 장애를 503 으로 노출 (실습 03 시나리오 A/B/C).
     */
    @PostMapping("/sync")
    public ResponseEntity<String> publishSync(@RequestBody OrderRequest request) {
        try {
            OrderEvent event = new OrderEvent(request.orderId(), request.status(), request.amount());
            producer.sendSync(event);
            return ResponseEntity.ok("delivered");
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (IllegalStateException e) {
            return ResponseEntity.status(503).body("kafka unavailable");
        }
    }

    /**
     * outbox 패턴 발행 — Kafka 가 죽어 있어도 즉시 202.
     * 비즈니스 트랜잭션과 같이 DB 적재 → 별도 워커가 실제 Kafka 로 발행.
     */
    @PostMapping("/outbox")
    public ResponseEntity<String> publishViaOutbox(@RequestBody OrderRequest request) {
        try {
            OrderEvent event = new OrderEvent(request.orderId(), request.status(), request.amount());
            outboxService.placeOrder(event);
            return ResponseEntity.accepted().body("outbox queued: " + request.orderId());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        } catch (IllegalStateException e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    public record OrderRequest(String orderId, String status, long amount) {}
}
