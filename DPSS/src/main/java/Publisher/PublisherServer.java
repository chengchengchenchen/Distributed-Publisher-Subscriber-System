package Publisher;

import Directory.DirectoryService;
import Utils.Broker;
import Utils.Constant;
import Utils.Publisher;
import Utils.Topic;
import lombok.extern.slf4j.Slf4j;


import java.net.ServerSocket;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Scanner;

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

            Publisher publisher = new Publisher(publisherName, Constant.DIRECTORY_HOST, assignedPort);
            //publisher.connectToBroker(selectedBroker.getHost() + ":" + selectedBroker.getPort());

            // Command loop
            while (true) {
                System.out.println("\nPlease select command: create, publish, show, delete, exit");
                System.out.print("Command: ");
                String command = scanner.nextLine();
                String[] parts = command.split(" ", 3);
                String action = parts[0].toLowerCase();

                switch (action) {
                    case "create":
                        if (parts.length < 3) {
                            System.out.println("Usage: create {topic_id} {topic_name}");
                            continue;
                        }
                        String topicId = parts[1];
                        String topicName = parts[2];
                        Topic newTopic = new Topic(topicId, topicName);
                        //publisher.addTopic(newTopic);
                        System.out.println("Created topic: " + topicName);
                        break;
                    case "publish":
                        if (parts.length < 3) {
                            System.out.println("Usage: publish {topic_id} {message}");
                            continue;
                        }
                        topicId = parts[1];
                        String message = parts[2];
                        if (message.length() > 100) {
                            System.out.println("Message exceeds the 100 character limit.");
                            continue;
                        }
                        // Logic to publish the message to the broker goes here
                        System.out.println("Published message to topic ID: " + topicId);
                        break;
                    case "show":
                        if (parts.length < 2) {
                            System.out.println("Usage: show {topic_id}");
                            continue;
                        }
                        topicId = parts[1];
                        // Logic to get subscriber count for the topic goes here
                        System.out.println("Showing subscriber count for topic ID: " + topicId);
                        break;
                    case "delete":
                        if (parts.length < 2) {
                            System.out.println("Usage: delete {topic_id}");
                            continue;
                        }
                        //publisher.removeTopic(topicToDelete);
                        break;
                    case "exit":
                        log.info("Exiting...");
                        return;
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