package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBlockingDequeReactive;
import org.redisson.api.RListReactive;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ru.vadim.messengercore.entity.Message;

import java.net.URI;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageServiceImpl implements WebSocketHandler {

    private final RedissonReactiveClient client;

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        String room = getChatRoomName(webSocketSession);

        RTopicReactive topic = this.client.getTopic("chat:room:" + room, StringCodec.INSTANCE);
        RListReactive<String> list = this.client.getList("chat:history:" + room, StringCodec.INSTANCE);

        // subscribe
        Mono<Void> receiveMessage = webSocketSession.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .doOnNext(message -> log.info("Send message: {}", message))
                .concatMap(message -> list.add(message).then(topic.publish(message)))
                .doOnError(message -> log.error("Error int subscriber{}", String.valueOf(message)))
                .doFinally(s -> System.out.println("Subscriber finally " + s))
                .then();

        // publisher
        Flux<WebSocketMessage> outgoingMessages = topic.getMessages(String.class)
                .startWith(list.iterator())
                .map(webSocketSession::textMessage)
                .doOnNext(message -> log.info("Get message: {}", message.getPayloadAsText()))
                .doOnError(message -> log.error("Error in publisher {}", String.valueOf(message)))
                .doFinally(s -> log.info("publisher finally {}", s));

        Mono<Void> sendMessages = webSocketSession.send(outgoingMessages);

        return Mono.when(receiveMessage, sendMessages)
                .doFinally(signal -> log.info("WebSocket session finished: room={}, session={}, signal={}", room, webSocketSession.getId(), signal));
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
