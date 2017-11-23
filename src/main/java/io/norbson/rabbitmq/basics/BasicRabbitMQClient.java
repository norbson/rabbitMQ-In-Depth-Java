package io.norbson.rabbitmq.basics;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class BasicRabbitMQClient {

    private static final Logger logger = LoggerFactory.getLogger(BasicRabbitMQClient.class);

    private final ConnectionFactory connectionFactory;
    private final String exchange;
    private final String queue;
    private final String binding;

    public BasicRabbitMQClient(String exchange, String queue, String binding) {
        this.exchange = exchange;
        this.queue = queue;
        this.binding = binding;
        this.connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
    }

    public <T> T executeOnChannel(Function<Channel, T> action) throws IOException, TimeoutException {
        try (Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel()) {
                channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT);
                channel.queueDeclare(queue, false, false, false, null);
                channel.queueBind(queue, exchange, binding);
                T result = action.apply(channel);
                logger.info("Action executed");
                return result;
        }
    }

    public static Function<Channel, String> consume(String queueName) {
        return channel -> {
            try {
                 return new String(channel.basicGet(queueName, true).getBody());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static Function<Channel, Void> publish(String exchange, String routingKey, String message) {
        return channel -> {
            try {
                channel.basicPublish(exchange, routingKey, null, message.getBytes());
                return null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
