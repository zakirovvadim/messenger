package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RListReactive;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageReceiverServiceImpl {

    private final RedissonReactiveClient client;

    Mono<Void> receiveMessage(WebSocketSession webSocketSession,RTopicReactive topic, RListReactive<String> list) {
        // subscribe
        return webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(message -> log.info("Send message: {}", message))
                .concatMap(message -> list.add(message).then(topic.publish(message)))
                .doOnError(message -> log.error("Error int subscriber{}", String.valueOf(message)))
                .doFinally(s -> System.out.println("Subscriber finally " + s))
                .then();
    }

    private String getChatRoomName(WebSocketSession socketSession){
        URI uri = socketSession.getHandshakeInfo().getUri();
        return UriComponentsBuilder.fromUri(uri)
                .build()
                .getQueryParams()
                .toSingleValueMap()
                .getOrDefault("room", "default");
    }
}
