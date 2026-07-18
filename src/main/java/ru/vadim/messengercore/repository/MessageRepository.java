package ru.vadim.messengercore.repository;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import ru.vadim.messengercore.entity.Message;

public interface MessageRepository extends ReactiveCrudRepository<Message, Long> {
    Flux<Message> findMessageByRoomIdOrderByIdDesc(Long roomId);
}
