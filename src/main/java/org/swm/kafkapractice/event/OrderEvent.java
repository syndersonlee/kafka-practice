package org.swm.kafkapractice.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class OrderEvent {

    private final String orderId;
    private final String status;     // CREATED / PAID / SHIPPED
    private final long amount;

    @JsonCreator
    public OrderEvent(
            @JsonProperty("orderId") String orderId,
            @JsonProperty("status") String status,
            @JsonProperty("amount") long amount) {
        if (orderId == null || orderId.isBlank()) {
            throw new IllegalArgumentException("orderId must not be blank");
        }
        if (status == null || status.isBlank()) {
            throw new IllegalArgumentException("status must not be blank");
        }
        this.orderId = orderId;
        this.status = status;
        this.amount = amount;
    }
}
