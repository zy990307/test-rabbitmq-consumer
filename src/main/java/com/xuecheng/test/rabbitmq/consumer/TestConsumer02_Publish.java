package com.xuecheng.test.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TestConsumer02_Publish {

    private final  static String EXCHANGE_FANOUT_INFORM="EXCHANGE_FANOUT_INFORM";
    private final  static String QUEUE_INFORM_SMS="QUEUE_INFORM_SMS";
    private final static String QUEUE_INFORM_EMAIL="QUEUE_INFORM_EMAIL";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("106.12.211.16");
        factory.setPort(5672);
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //创建交换机
        channel.exchangeDeclare(EXCHANGE_FANOUT_INFORM,BuiltinExchangeType.FANOUT);
        channel.queueDeclare(QUEUE_INFORM_EMAIL,true,false,false,null);
        channel.queueDeclare(QUEUE_INFORM_SMS,true,false,false,null);

        channel.queueBind(QUEUE_INFORM_SMS,EXCHANGE_FANOUT_INFORM,QUEUE_INFORM_SMS,null);
        channel.queueBind(QUEUE_INFORM_EMAIL,EXCHANGE_FANOUT_INFORM,QUEUE_INFORM_EMAIL,null);
        DefaultConsumer smsConsumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("sms消费者:--------------------");
                String routingKey = envelope.getRoutingKey();
                System.out.println("RoutingKey:"+routingKey);
                System.out.println(new String(body,"utf-8"));
            }
        };
        DefaultConsumer emailConsumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("email消费者:--------------------");
                String routingKey = envelope.getRoutingKey();
                System.out.println("RoutingKey:"+routingKey);
                System.out.println(new String(body,"utf-8"));
            }
        };

        channel.basicConsume(QUEUE_INFORM_EMAIL,true,emailConsumer);
        channel.basicConsume(QUEUE_INFORM_SMS,true,smsConsumer);
    }
}
