package ru.vadim.messengercore.repository;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import ru.vadim.messengercore.entity.User;

public interface UserRepository extends ReactiveCrudRepository<User, Long> {
}
