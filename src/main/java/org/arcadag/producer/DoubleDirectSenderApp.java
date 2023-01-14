package org.arcadag.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DoubleDirectSenderApp {
    private static final String EXCHANGE_NAME = "DoubleDirect";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))){
                String line;
                while (!(line = reader.readLine()).equals("end")) {
                    String[] data = line.split("\s", 2);
                    channel.basicPublish(EXCHANGE_NAME, data[0], null, data[1].getBytes("UTF-8"));
                }

            }

//            channel.basicPublish(EXCHANGE_NAME, "php", null, "php msg".getBytes("UTF-8"));
//            channel.basicPublish(EXCHANGE_NAME, "c++", null, "c++ msg".getBytes("UTF-8"));
//            channel.basicPublish(EXCHANGE_NAME, "java", null, "java msg".getBytes("UTF-8"));
            System.out.println("OK");
        }
    }
}
