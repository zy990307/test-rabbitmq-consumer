package com.xuecheng.test.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TestConsumer01 {
    private final  static String TEST_QUEUS = "TEST_QUEUE";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setHost("106.12.211.16");
        factory.setPort(5672);
        factory.setVirtualHost("/");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(TEST_QUEUS,true,false,false,null);

        System.out.println("consumer start");
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel){
            /**
             * 消费者接收消息调用此方法
             * @param consumerTag 消费者的标签，在channel.basicConsume()去指定
             * @param envelope 消息包的内容，可从中获取消息id，消息routingkey，交换机，消息和重传标志(收到消息失败后是否需要重新发送)
             * @param properties
             * @param body
             * @throws IOException
             */
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("消费者标签:"+consumerTag);
                //获取消息id
                long deliveryTag = envelope.getDeliveryTag();
                System.out.println("消息id:"+deliveryTag);
                //获取交换机
                String exchange = envelope.getExchange();
                System.out.println("交换机:"+exchange);
                //获取routingkey
                String routingKey = envelope.getRoutingKey();
                System.out.println("RoutingKey:"+routingKey);
                String s = new String(body, "utf-8");
                System.out.println("消息内容:"+s);
            }
        };

        channel.basicConsume(TEST_QUEUS,true,defaultConsumer);
    }
}
