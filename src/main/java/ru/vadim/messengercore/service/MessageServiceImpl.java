package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageServiceImpl implements WebSocketHandler {

    private final MessageReceiverServiceImpl receiverService;
    private final MessageSenderServiceImpl senderService;

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        Long room = getChatRoomName(webSocketSession);
        // subscribe
        Mono<Void> receiveMessage = receiverService.receiveMessage(webSocketSession, room);
        // publisher
        Flux<WebSocketMessage> outgoingMessages = senderService.sendMessage(webSocketSession, room);

        Mono<Void> sendMessages = webSocketSession.send(outgoingMessages);
        return Mono.when(receiveMessage, sendMessages)
                .doFinally(signal -> log.info("WebSocket session finished: room={}, session={}, signal={}", room, webSocketSession.getId(), signal));
    }

    private Long getChatRoomName(WebSocketSession socketSession) {
        URI uri = socketSession.getHandshakeInfo().getUri();
        String roomId = UriComponentsBuilder.fromUri(uri)
                .build()
                .getQueryParams()
                .toSingleValueMap()
                .getOrDefault("room", "1");
        return Long.valueOf(roomId);
    }
}
