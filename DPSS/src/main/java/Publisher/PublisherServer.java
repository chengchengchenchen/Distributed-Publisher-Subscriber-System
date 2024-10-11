/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
package Publisher;

import Directory.DirectoryService;
import Broker.BrokerInterface;
import Utils.Broker;
import Utils.Constant;
import Utils.Publisher;
import Utils.Topic;
import Utils.Message;
import lombok.extern.slf4j.Slf4j;


import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

@Slf4j
public class PublisherServer {
    public static void main(String[] args) {
        try {
            if (args.length < 1) {
                log.error("Usage: java PublisherServer <publisherName>");
                return;
            }
            String publisherName = args[0];

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
            PublisherImpl publisherImpl = new PublisherImpl();
            Registry registryPublisher = LocateRegistry.createRegistry(assignedPort);
            registryPublisher.rebind(publisherName, publisherImpl);

            // Connect to Selected Broker
            Registry registryBroker = LocateRegistry.getRegistry(selectedBroker.getHost(), selectedBroker.getPort());
            BrokerInterface brokerStub = (BrokerInterface) registryBroker.lookup(selectedBroker.getName());
            Publisher publisher = new Publisher(publisherName, Constant.DIRECTORY_HOST, assignedPort);
            brokerStub.newPublisherConnected(publisher);

            // Command loop
            while (true) {
                System.out.println("\nPlease select command: create, publish, show, delete");
                System.out.print("Command: ");
                String command = scanner.nextLine();
                String[] parts = command.split(" ", 3);
                String action = parts[0].toLowerCase();
                String topicId;
                String topicName;

                switch (action) {
                    case "create":
                        if (parts.length < 3) {
                            System.out.println("Usage: create {topic_id} {topic_name}");
                            continue;
                        }
                        topicId = parts[1];
                        topicName = parts[2];
                        Topic newTopic = new Topic(topicId, topicName, publisher);
                        boolean isCreated = brokerStub.createTopic(publisher, newTopic);
                        if (isCreated) {
                            log.info("Topic created successfully: " + topicName);
                        } else {
                            log.info("Failed to create topic: " + topicName);
                        }
                        break;

                    case "publish":
                        if (parts.length < 3) {
                            System.out.println("Usage: publish {topic_id} {message}");
                            continue;
                        }
                        topicId = parts[1];
                        String payload = parts[2];
                        if (payload.length() > 100) {
                            log.warn("Message exceeds the 100 character limit.");
                            continue;
                        }

                        // check whether this publisher has the topic
                        Map<Topic, Integer> publisherTopics = brokerStub.getTopicDetails(publisher, System.currentTimeMillis() + publisher.getName());
                        Topic foundTopic = publisherTopics.keySet().stream()
                                .filter(t -> t.getTopicId().equals(topicId))
                                .findFirst()
                                .orElse(null);

                        if (foundTopic != null) {
                            Message message = new Message(foundTopic, payload);
                            brokerStub.publishMessage(publisher, message, System.currentTimeMillis() + publisher.getName());
                            log.info("Message published to topic ID: " + topicId);
                        } else {
                            log.warn("Publisher does not own topic ID: " + topicId);
                        }
                        break;

                    case "show":
                        if (parts.length < 2) {
                            Map<Topic, Integer> allTopicDetails = brokerStub.getTopicDetails(publisher, System.currentTimeMillis() + publisher.getName());
                            if (allTopicDetails.isEmpty()) {
                                log.info("No subscribers");
                            } else {
                                for (Map.Entry<Topic, Integer> entry : allTopicDetails.entrySet()) {
                                    Topic topic = entry.getKey();
                                    int subscriberCount = entry.getValue();
                                    log.info("[{}] [{}] [{}]", topic.getTopicId(), topic.getName(), subscriberCount);
                                }
                            }
                        } else {
                            // show specified topic
                            topicId = parts[1];
                            Map<Topic, Integer> topicDetails = brokerStub.getTopicDetails(publisher, System.currentTimeMillis() + publisher.getName());
                            Topic matchedTopic = null;
                            int subscriberCount = -1;

                            for (Map.Entry<Topic, Integer> entry : topicDetails.entrySet()) {
                                if (entry.getKey().getTopicId().equals(topicId)) {
                                    matchedTopic = entry.getKey();
                                    subscriberCount = entry.getValue();
                                    break;
                                }
                            }

                            if (matchedTopic == null) {
                                log.error("Not found Topic ID: " + topicId);
                            } else {
                                log.info("[{}}] [{}] [{}]", matchedTopic.getTopicId(), matchedTopic.getName(), subscriberCount);
                            }
                        }
                        break;

                    case "delete":
                        if (parts.length < 2) {
                            System.out.println("Usage: delete {topic_id}");
                            continue;
                        }
                        topicId = parts[1];

                        boolean isDeleted = brokerStub.deleteTopic(publisher, new Topic(topicId), System.currentTimeMillis() + publisher.getName());
                        if (isDeleted) {
                            log.info("Topic deleted successfully: " + topicId);
                        } else {
                            log.info("Failed to delete topic: " + topicId);
                        }
                        break;

                    default:
                        log.info("Unknown command. Please try again.");
                        break;
                }
            }
        } catch (Exception e) {
            log.error("Failed to start PublisherServer", e);
        }
    }
}