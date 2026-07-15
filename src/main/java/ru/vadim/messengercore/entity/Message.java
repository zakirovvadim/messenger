package ru.vadim.messengercore.entity;

public record Message(
        String data,
        User recipient,
        User sender,
        Long roomId
) {
}
