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
    private final MessageReceiverServiceImpl receiverService;
    private final MessageSenderServiceImpl senderService;

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        String room = getChatRoomName(webSocketSession);

        RTopicReactive topic = this.client.getTopic("chat:room:" + room, StringCodec.INSTANCE);
        RListReactive<String> list = this.client.getList("chat:history:" + room, StringCodec.INSTANCE);

        // subscribe
        Mono<Void> receiveMessage = receiverService.receiveMessage(webSocketSession, topic, list);
        // publisher
        Flux<WebSocketMessage> outgoingMessages = senderService.sendMessage(webSocketSession, topic, list);

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
