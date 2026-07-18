package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RListReactive;
import org.redisson.api.RTopicReactive;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageSenderServiceImpl {

    Flux<WebSocketMessage> sendMessage(WebSocketSession webSocketSession, RTopicReactive topic, RListReactive<String> list) {
        return topic.getMessages(String.class)
                .startWith(list.iterator())
                .map(webSocketSession::textMessage)
                .doOnNext(message -> log.info("Get message: {}", message.getPayloadAsText()))
                .doOnError(message -> log.error("Error in publisher {}", String.valueOf(message)))
                .doFinally(s -> log.info("publisher finally {}", s));
    }
}
