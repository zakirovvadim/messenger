package ru.vadim.messengercore.repository;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import ru.vadim.messengercore.entity.Message;

public interface MessageRepository extends ReactiveCrudRepository<Message, Long> {
}
