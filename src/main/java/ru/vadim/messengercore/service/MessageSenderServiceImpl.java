package ru.vadim.messengercore.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RTopicReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import ru.vadim.messengercore.entity.Message;
import ru.vadim.messengercore.repository.MessageRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageSenderServiceImpl {

    private final MessageRepository messageRepository;
    private final RedissonReactiveClient client;


    Flux<WebSocketMessage> sendMessage(WebSocketSession webSocketSession, Long roomId) {
        RTopicReactive topic = this.client.getTopic("chat:room:" + roomId, StringCodec.INSTANCE);

        Flux<String> oldRoomMessages = messageRepository.findMessageByRoomIdOrderByIdDesc(roomId)
                .map(Message::data);

        return topic.getMessages(String.class)
                .startWith(oldRoomMessages)
                .map(webSocketSession::textMessage)
                .doOnNext(message -> log.info("Get message: {}", message.getPayloadAsText()))
                .doOnError(message -> log.error("Error in publisher {}", String.valueOf(message)))
                .doFinally(s -> log.info("publisher finally {}", s));
    }
}
