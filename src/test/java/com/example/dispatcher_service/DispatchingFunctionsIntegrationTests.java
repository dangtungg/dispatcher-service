package com.example.dispatcher_service;

import com.example.dispatcher_service.func.OrderAcceptedMessage;
import com.example.dispatcher_service.func.OrderDispatchedMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.test.FunctionalSpringBootTest;
import org.springframework.messaging.Message;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

@FunctionalSpringBootTest
@Disabled("These tests are only necessary when using the functions alone (no bindings)")
class DispatchingFunctionsIntegrationTests {

    @Autowired
    private FunctionCatalog catalog;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void packOrder() {
        Function<OrderAcceptedMessage, Long> pack = catalog.lookup(Function.class, "pack");
        long orderId = 121;
        assertThat(pack.apply(new OrderAcceptedMessage(orderId))).isEqualTo(orderId);
    }

    @Test
    void labelOrder() {
        Function<Flux<Long>, Flux<OrderDispatchedMessage>> label = catalog.lookup(Function.class, "label");
        Flux<Long> orderId = Flux.just(121L);

        StepVerifier.create(label.apply(orderId))
                .expectNextMatches(dispatchedOrder ->
                        dispatchedOrder.equals(new OrderDispatchedMessage(121L)))
                .verifyComplete();
    }

    @Test
    void packAndLabelOrder() {
        // Trong cách này, chúng ta đang giả định rằng function kết hợp pack|label trả về trực tiếp Flux<OrderDispatchedMessage>.
        // Tuy nhiên, trong thực tế, Spring Cloud Function đang bọc kết quả trong một Message<?> object,
        // điều này dẫn đến lỗi ClassCastException khi cố gắng cast GenericMessage thành OrderDispatchedMessage.

        /*Function<OrderAcceptedMessage, Flux<OrderDispatchedMessage>> packAndLabel = catalog.lookup(Function.class, "pack|label");
        long orderId = 121;

        StepVerifier.create(packAndLabel.apply(new OrderAcceptedMessage(orderId)))
                .expectNextMatches(dispatchedOrder ->
                        dispatchedOrder.equals(new OrderDispatchedMessage(orderId)))
                .verifyComplete();*/

        // Bằng cách khai báo Flux<Message<?>>, chúng ta đang xử lý đúng kiểu dữ liệu mà Spring Cloud Function trả về.
        // Trong expectNextMatches, chúng ta kiểm tra và xử lý payload của Message<?>:
        // - Kiểm tra xem payload có phải là byte[] không (JSON serialized).
        // - Sử dụng ObjectMapper để deserialize byte[] thành OrderDispatchedMessage.
        // - So sánh OrderDispatchedMessage đã được deserialized với giá trị mong đợi.
        Function<OrderAcceptedMessage, Flux<Message<?>>> packAndLabel = catalog.lookup(Function.class, "pack|label");
        long orderId = 121;

        StepVerifier.create(packAndLabel.apply(new OrderAcceptedMessage(orderId)))
                .expectNextMatches(message -> {
                    try {
                        if (message.getPayload() instanceof byte[]) {
                            OrderDispatchedMessage dispatchedOrder = objectMapper.readValue((byte[]) message.getPayload(), OrderDispatchedMessage.class);
                            return dispatchedOrder.equals(new OrderDispatchedMessage(orderId));
                        }
                        return false;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .verifyComplete();
    }

}
