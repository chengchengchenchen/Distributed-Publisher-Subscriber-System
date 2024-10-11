/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Subscriber;

import Broker.BrokerInterface;
import Directory.DirectoryService;
import Utils.*;
import lombok.extern.slf4j.Slf4j;

import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

@Slf4j
public class SubscriberServer {
    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                log.error("Usage: java SubscriberServer <subscriberName>");
                return;
            }
            String subscriberName = args[0];

            // Connect to Directory Service
            Registry registry = LocateRegistry.getRegistry(Constant.DIRECTORY_HOST, Constant.DIRECTORY_PORT);
            DirectoryService directoryService = (DirectoryService) registry.lookup("DirectoryService");
            List<Broker> availableBrokers = directoryService.getRegisteredBrokers();
            if (availableBrokers.isEmpty()) {
                log.error("No brokers available. Exiting...");
                return;
            }

            // Select a broker to connect to by name
            Scanner scanner = new Scanner(System.in);
            System.out.println("Available brokers:");
            for (Broker broker : availableBrokers) {
                System.out.printf("%s (%s:%d)%n", broker.getName(), broker.getHost(), broker.getPort());
            }
            System.out.print("Enter the name of the broker to connect to: ");
            String brokerName = scanner.nextLine();

            Broker selectedBroker = availableBrokers.stream()
                    .filter(broker -> broker.getName().equals(brokerName))
                    .findFirst()
                    .orElse(null);

            if (selectedBroker == null) {
                log.error("Invalid broker selection. Exiting...");
                return;
            }

            // Assign a port for RMI
            int assignedPort;
            try (ServerSocket serverSocket = new ServerSocket(0)) {
                assignedPort = serverSocket.getLocalPort();
            }
            SubscriberImpl subscriberImpl = new SubscriberImpl();
            Registry registrySubscriber = LocateRegistry.createRegistry(assignedPort);
            registrySubscriber.rebind(subscriberName, subscriberImpl);

            // Connect to Selected Broker
            Registry registryBroker = LocateRegistry.getRegistry(selectedBroker.getHost(), selectedBroker.getPort());
            BrokerInterface brokerStub = (BrokerInterface) registryBroker.lookup(selectedBroker.getName());
            Subscriber subscriber = new Subscriber(subscriberName, Constant.DIRECTORY_HOST, assignedPort);
            brokerStub.newSubscriberConnected(subscriber);

            // Command loop
            while (true) {
                System.out.println("\nPlease select command: list, sub, current, unsub");
                System.out.print("Command: ");
                String command = scanner.nextLine();
                String[] parts = command.split(" ", 2);
                String action = parts[0].toLowerCase();
                String topicId;

                switch (action) {
                    case "list":
                        Set<Topic> allTopics = brokerStub.listAllTopics(System.currentTimeMillis() + subscriber.getName());
                        if (allTopics.isEmpty()) {
                            log.info("No available topics.");
                        } else {
                            allTopics.forEach((topic) -> {
                                log.info("[{}] [{}] [{}]", topic.getTopicId(), topic.getName(), topic.getPublisher().getName());
                            });
                        }
                        break;

                    case "sub":
                        if (parts.length < 2) {
                            System.out.println("Usage: sub {topic_id}");
                            continue;
                        }
                        topicId = parts[1];
                        boolean isSubscribed = brokerStub.subscribeToTopic(subscriber, topicId);
                        if (isSubscribed) {
                            log.info("Subscribed to topic: " + topicId);
                        } else {
                            log.warn("Failed to subscribe to topic: " + topicId);
                        }
                        break;

                    case "current":
                        Set<Topic> currentSubscriptions = brokerStub.getSubscribedTopics(subscriber);
                        if (currentSubscriptions.isEmpty()) {
                            log.info("No current subscriptions.");
                        } else {
                            currentSubscriptions.forEach(topic -> {
                                log.info("[{}] [{}] [{}]", topic.getTopicId(), topic.getName(), topic.getPublisher().getName());
                            });
                        }
                        break;

                    case "unsub":
                        // 取消订阅 Topic
                        if (parts.length < 2) {
                            System.out.println("Usage: unsub {topic_id}");
                            continue;
                        }
                        topicId = parts[1];
                        boolean isUnsubscribed = brokerStub.unsubscribeFromTopic(subscriber, topicId);
                        if (isUnsubscribed) {
                            log.info("Unsubscribed from topic: " + topicId);
                        } else {
                            log.warn("Failed to unsubscribe from topic: " + topicId);
                        }
                        break;

                    default:
                        log.info("Unknown command. Please try again.");
                        break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to start SubscriberServer", e);
        }
    }
}
