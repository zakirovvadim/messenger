package ru.vadim.messengercore.entity;

import java.util.Set;

public record User(
        Long userId,
        Set<Integer> rooms
) {
}
