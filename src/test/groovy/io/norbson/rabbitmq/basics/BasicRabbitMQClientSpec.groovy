package io.norbson.rabbitmq.basics

import spock.lang.Specification
import spock.lang.Subject

import static BasicRabbitMQClient.consume
import static BasicRabbitMQClient.publish

class BasicRabbitMQClientSpec extends Specification {

    String routingKey = 'basic-routing-key'
    String exchange = 'basic-exchange'
    String queue = 'basic-queue'

    @Subject
    BasicRabbitMQClient rabbitClient = new BasicRabbitMQClient(exchange, queue, routingKey)

    def 'send message to queue then consume' () {
        given:
            String message = 'Hello World!'
        when:
            rabbitClient.executeOnChannel(publish(exchange, routingKey, message))
        then:
            rabbitClient.executeOnChannel(consume(queue)) == message
    }
}
