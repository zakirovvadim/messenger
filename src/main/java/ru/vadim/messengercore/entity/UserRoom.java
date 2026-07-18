package ru.vadim.messengercore.entity;

import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@Table("user_room")
public record UserRoom(
        @Column("user_id") Long userId,
        @Column("room_id") Long roomId
) {
}