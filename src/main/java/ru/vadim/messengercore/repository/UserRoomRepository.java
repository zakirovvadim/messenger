package ru.vadim.messengercore.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import ru.vadim.messengercore.entity.UserRoom;

public interface UserRoomRepository extends ReactiveCrudRepository<UserRoom, Long> {
}
