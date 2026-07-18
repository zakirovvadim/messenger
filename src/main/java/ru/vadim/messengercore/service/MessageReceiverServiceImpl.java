package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;
import ru.vadim.messengercore.entity.Message;
import ru.vadim.messengercore.repository.MessageRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageReceiverServiceImpl {

    private final RedissonReactiveClient client;
    private final MessageRepository messageRepository;

    Mono<Void> receiveMessage(WebSocketSession webSocketSession, Long roomId) {
        RTopicReactive topic = this.client.getTopic("chat:room:" + roomId, StringCodec.INSTANCE);

        // subscribe
        return webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(message -> log.info("Send message: {}", message))
                .concatMap(message -> {
                    Message messageEntity = new Message(null, message, 1L, 2L, roomId);
                    return messageRepository.save(messageEntity)
                            .then(topic.publish(message));
                })
                .doOnError(message -> log.error("Error int subscriber{}", String.valueOf(message)))
                .doFinally(signal -> log.info("Subscriber finally {}", signal))
                .then();
    }
}
