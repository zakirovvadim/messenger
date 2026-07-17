package ru.vadim.messengercore.configuration;

import org.redisson.api.RedissonReactiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import ru.vadim.messengercore.service.MessageServiceImpl;

import java.util.Map;

@Configuration
public class RedissonConfiguration {

    @Autowired
    private MessageServiceImpl chatRoomService;

    @Bean
    public HandlerMapping handlerMapping() {
        return new SimpleUrlHandlerMapping(Map.of("/chat", chatRoomService), -1);
    }

}
