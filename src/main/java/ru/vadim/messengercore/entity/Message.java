package ru.vadim.messengercore.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("message")
public record Message(
        @Id Long id,
        String data,
        Long recipientId,
        Long senderId,
        Long roomId
) {
}