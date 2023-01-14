package org.arcadag.consumer;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DoubleDirectReceiver {
    private static final String EXCHANGE_NAME = "DoubleDirect";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        System.out.println("My queue name: " + queueName);

//        channel.queueBind(queueName, EXCHANGE_NAME, "php");
//        channel.queueBind(queueName, EXCHANGE_NAME, "java");

        System.out.println(" [*] Waiting for message");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), "UTF-8");
          System.out.println(" [x] Received '" + message + "'");
          System.out.println(Thread.currentThread().getName());
        };

//        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line; // add topic (java, c++), remove topic
            System.out.println("to add a topic, type - add your_topic, to remove, type - remove your_topic, or type - end to exit. available php, java, c++ options");
            while (!(line = reader.readLine()).equals("end")) {
                String[] data = line.split("\s", 2);
                if (data[0].equals("add")) {
                    channel.queueBind(queueName, EXCHANGE_NAME, data[1]);
                }
                if (data[0].equals("remove")) {
                    channel.queueUnbind(queueName, EXCHANGE_NAME, data[1]);
                }
                channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            }
        }
    }
}
