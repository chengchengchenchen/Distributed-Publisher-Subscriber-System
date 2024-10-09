package Broker;

import Directory.DirectoryService;
import Publisher.PublisherInterface;
import Subscriber.SubscriberInterface;
import Utils.*;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

// Broker Implementation
@Slf4j
@Getter
public class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private final Broker broker;
    private final List<Subscriber> connectedSubscribers;
    private final List<Publisher> connectedPublishers;
    private final List<Broker> connectedBrokers;
    private final long heartbeatInterval = 5000L; // heartbeat per 5 sec
    public BrokerImpl(Broker broker) throws RemoteException {
        super();
        this.broker = broker;
        this.connectedPublishers = new ArrayList<>();
        this.connectedSubscribers = new ArrayList<>();
        this.connectedBrokers = new ArrayList<>();
    }

    @Override
    public void newBrokerRegistered(Broker broker) throws RemoteException {
        log.info("New broker registered: {}", broker.getName());
        if (connectedBrokers.stream().noneMatch(b -> b.getName().equals(broker.getName()))) {
            connectedBrokers.add(broker);
            log.info("Connected to new broker: {}", broker.getName());
        }
    }

    @Override
    public void newPublisherConnected(Publisher publisher) throws RemoteException {
        log.info("New publisher connected: {}", publisher.getName());

        if (connectedPublishers.stream().noneMatch(p -> p.getName().equals(publisher.getName()))) {
            connectedPublishers.add(publisher);
            log.info("Publisher {} registered successfully.", publisher.getName());

            // register heartbeat
            startHeartbeatCheck(publisher);
        }else {
            log.warn("Publisher {} is already connected.", publisher.getName());
        }
    }

    // register heartbeat
    private void startHeartbeatCheck(Publisher publisher) {
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    Registry registry = LocateRegistry.getRegistry(publisher.getHost(), publisher.getPort());
                    PublisherInterface publisherStub = (PublisherInterface) registry.lookup(publisher.getName());

                    if (publisherStub.isAlive()) {
                        log.info("Publisher {} is alive", publisher.getName());
                    } else {
                        log.warn("Publisher {} did not respond to heartbeat, removing...", publisher.getName());
                        handlePublisherCrash(publisher);
                        timer.cancel();
                    }

                } catch (Exception e) {
                    log.error("Failed to reach publisher {} during heartbeat check, removing...", publisher.getName());
                    handlePublisherCrash(publisher);
                    timer.cancel();
                }
            }
        }, 0, heartbeatInterval);
    }

    private void handlePublisherCrash(Publisher publisher) {
        // delete all topics of publisher
        List<Topic> topicsToRemove = new ArrayList<>();
        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName())) {
                topicsToRemove.addAll(connectedPublisher.getTopics());
                break;
            }
        }

        for (Topic topic : topicsToRemove) {
            try {
                deleteTopic(publisher, topic, new ArrayList<>());
            } catch (RemoteException e) {
                log.error("Failed to delete topic {} for publisher {}", topic.getTopicId(), publisher.getName(), e);
            }
        }

        connectedPublishers.remove(publisher);

        log.info("Publisher {} and all its topics have been removed", publisher.getName());
    }
    // Publisher Create Topic
    @Override
    public boolean createTopic(Publisher publisher, Topic topic) throws RemoteException {
        log.info("Publisher {} is creating topic {}", publisher.getName(), topic.getName());

        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName()) &&
                    connectedPublisher.getHost().equals(publisher.getHost()) &&
                    connectedPublisher.getPort() == publisher.getPort()) {

                // update topic
                connectedPublisher.addTopic(topic);

                log.info("Topic {} created by Publisher {} in Broker", topic.getName(), connectedPublisher.getName());
                return true;
            }
        }
        log.warn("Publisher {} not found in connected publishers", publisher.getName());
        return false;
    }

    //TODO: fix
    @Override
    public Map<Topic, Integer> getTopicDetails(Publisher publisher, List<String> processedBrokers) throws RemoteException {
        String brokerName = broker.getName();

        // check to avoid loop
        if (processedBrokers.contains(brokerName)) {
            log.info("Broker {} already processed this request, skipping.", brokerName);
            return new HashMap<>();
        }

        processedBrokers.add(brokerName);
        Map<Topic, Integer> topicDetails = new HashMap<>();

        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName())) {
                for (Topic topic : connectedPublisher.getTopics()) {
                    int count = 0;
                    for (Subscriber subscriber : connectedSubscribers) {
                        if (subscriber.getTopics().contains(topic)) {
                            count++;
                        }
                    }
                    topicDetails.put(topic, count);
                }
                break;
            }
        }

        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                Map<Topic, Integer> otherBrokerDetails = brokerStub.getTopicDetails(publisher, processedBrokers);

                for (Map.Entry<Topic, Integer> entry : otherBrokerDetails.entrySet()) {
                    Topic topic = entry.getKey();
                    int count = entry.getValue();

                    // If the topic already exists, sum the subscriber counts. Otherwise, add it.
                    topicDetails.merge(topic, count, Integer::sum);
                }
            } catch (Exception e) {
                log.error("Failed to forward delete topic operation to broker {}", connectedBroker.getName(), e);
            }
        }

        return topicDetails;
    }

    @Override
    public void publishMessage(Publisher publisher, Message message, List<String> processedBrokers) throws RemoteException {
        String brokerName = broker.getName();
        if (processedBrokers.contains(brokerName)) {
            return;
        }
        processedBrokers.add(brokerName);

        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName())) {
                for (Topic topic : connectedPublisher.getTopics()) {
                    if (topic.getTopicId().equals(message.getTopic().getTopicId())) {
                        // Publish message to subscribers
                        for (Subscriber subscriber : connectedSubscribers) {
                            if (subscriber.getTopics().contains(topic)) {
                                try {
                                    Registry registry = LocateRegistry.getRegistry(subscriber.getHost(), subscriber.getPort());
                                    SubscriberInterface subscriberStub = (SubscriberInterface) registry.lookup(subscriber.getName());
                                    subscriberStub.receiveMessage(message);
                                    log.info("Message sent to subscriber {} for topic {}", subscriber.getName(), topic.getName());
                                } catch (Exception e) {
                                    log.error("Failed to send message to subscriber {}", subscriber.getName(), e);
                                }
                            }
                        }

                        // broadcast
                        for (Broker connectedBroker : connectedBrokers) {
                            try {
                                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                                brokerStub.publishMessage(publisher, message, processedBrokers);
                            } catch (Exception e) {
                                log.error("Failed to broadcast message to broker {}", connectedBroker.getName(), e);
                            }
                        }
                        return;
                    }
                }
            }
        }

        log.warn("Publisher {} does not have topic {}", publisher.getName(), message.getTopic().getTopicId());
    }

    // Publisher Delete Topic
    @Override
    public boolean deleteTopic(Publisher publisher, Topic topic, List<String> processedBrokers) throws RemoteException {
        String brokerName = broker.getName();

        // check to avoid loop
        if (processedBrokers.contains(brokerName)) {
            log.info("Broker {} already processed this request, skipping.", brokerName);
            return false;
        }

        // add current Broker into processedBrokers
        processedBrokers.add(brokerName);
        log.info("Publisher {} is deleting topic {}", publisher.getName(), topic.getName());

        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName()) &&
                    connectedPublisher.getHost().equals(publisher.getHost()) &&
                    connectedPublisher.getPort() == publisher.getPort()) {

                // Update Publisher's Topic list
                connectedPublisher.removeTopic(topic);

                // Remove the topic from all connected subscribers
                for (Subscriber subscriber : connectedSubscribers) {
                    if (subscriber.getTopics().contains(topic)) {
                        subscriber.removeTopic(topic);
                        log.info("Subscriber {} unsubscribed from topic id: {}", subscriber.getName(), topic.getTopicId());

                        // Notify subscriber about topic deletion via RMI
                        try {
                            Registry registry = LocateRegistry.getRegistry(subscriber.getHost(), subscriber.getPort());
                            SubscriberInterface subscriberStub = (SubscriberInterface) registry.lookup(subscriber.getName());
                            subscriberStub.notifyTopicDeleted(topic);
                            log.info("Subscriber {} notified about topic id: {} deletion", subscriber.getName(), topic.getTopicId());
                        } catch (Exception e) {
                            log.error("Failed to notify subscriber {} about topic id: {} deletion", subscriber.getName(), topic.getTopicId(), e);
                        }
                    }
                }

                log.info("All subscribers unsubscribed from topic {}.", topic.getTopicId());

                // Broadcast delete operation to other brokers with updated processedBrokers list
                for (Broker connectedBroker : connectedBrokers) {
                    try {
                        Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                        BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                        brokerStub.deleteTopic(publisher, topic, processedBrokers);
                        log.info("Delete topic id: {} operation forwarded to broker {}", topic.getTopicId(), connectedBroker.getName());
                    } catch (Exception e) {
                        log.error("Failed to forward delete topic operation to broker {}", connectedBroker.getName(), e);
                    }
                }
                return true;
            }
        }

        log.warn("Publisher {} not found in connected publishers", publisher.getName());
        return false;
    }

    @Override
    public void newSubscriberConnected(Subscriber subscriber) throws RemoteException {
        log.info("New subscriber connected: {}", subscriber.getName());

        if (connectedSubscribers.stream().noneMatch(s -> s.getName().equals(subscriber.getName()))) {
            connectedSubscribers.add(subscriber);
            log.info("Subscriber {} registered successfully.", subscriber.getName());

            // register heartbeat
            startHeartbeatCheck(subscriber);
        } else {
            log.warn("Subscriber {} is already connected.", subscriber.getName());
        }
    }


    @Override
    public Map<Topic, String> listAllTopics(List<String> processedBrokers) throws RemoteException {
        String brokerName = broker.getName();

        // check to avoid loop
        if (processedBrokers.contains(brokerName)) {
            log.info("Broker {} already processed this request, skipping.", brokerName);
            return new HashMap<>();
        }

        processedBrokers.add(brokerName);
        Map<Topic, String> allTopics = new HashMap<>();


        for (Publisher connectedPublisher : connectedPublishers) {
            for (Topic topic : connectedPublisher.getTopics()) {
                allTopics.put(topic, connectedPublisher.getName());
            }
        }

        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                Map<Topic, String> otherBrokerTopics = brokerStub.listAllTopics(processedBrokers);
                allTopics.putAll(otherBrokerTopics);
            } catch (Exception e) {
                log.error("Failed to forward Broker {}", connectedBroker.getName(), e);
            }
        }

        return allTopics;
    }

    @Override
    public boolean subscribeToTopic(Subscriber subscriber, String topicId) throws RemoteException {
        Topic matchedTopic = null;

        // Search for the matching Topic
        outerLoop:
        for (Publisher connectedPublisher : connectedPublishers) {
            for (Topic topic : connectedPublisher.getTopics()) {
                if (topic.getTopicId().equals(topicId)) {
                    matchedTopic = topic;
                    break outerLoop;  // Exit loops
                }
            }
        }

        // If no matching Topic
        if (matchedTopic == null) {
            log.warn("Topic {} not found. Subscription failed for subscriber {}", topicId, subscriber.getName());
            return false;
        }

        // Check if the subscriber is already subscribed to the Topic
        for (Subscriber existingSubscriber : connectedSubscribers) {
            if (existingSubscriber.getName().equals(subscriber.getName())) {
                if (existingSubscriber.getTopics().contains(matchedTopic)) {
                    log.info("Subscriber {} is already subscribed to Topic {}", subscriber.getName(), matchedTopic.getTopicId());
                    return false;
                } else {
                    existingSubscriber.addTopic(matchedTopic);
                    log.info("Subscriber {} successfully subscribed to Topic {}", subscriber.getName(), matchedTopic.getTopicId());
                    return true;
                }
            }
        }
        log.warn("No subscriber {}", subscriber.getName());
        return false;
    }

    @Override
    public Set<Topic> getSubscribedTopics(Subscriber subscriber) throws RemoteException {
        for (Subscriber existingSubscriber : connectedSubscribers) {
            if (existingSubscriber.getName().equals(subscriber.getName())){
                log.info("Subscriber {} is in this broker", subscriber.getName());
                return existingSubscriber.getTopics();
            }
        }
        log.warn("Subscriber {} is not in this broker", subscriber.getName());
        return new HashSet<>();
    }

    @Override
    public boolean unsubscribeFromTopic(Subscriber subscriber, String topicId) throws RemoteException {
        for (Subscriber connectedSubscriber : connectedSubscribers) {
            if (connectedSubscriber.getName().equals(subscriber.getName())) {
                // Check if the subscriber is subscribed to the given topic
                for (Topic topic : connectedSubscriber.getTopics()) {
                    if (topic.getTopicId().equals(topicId)) {
                        // Remove
                        connectedSubscriber.removeTopic(topic);
                        log.info("Subscriber {} successfully unsubscribed from Topic {}", subscriber.getName(), topicId);
                        return true;
                    }
                }
                log.warn("Subscriber {} is not subscribed to Topic {}", subscriber.getName(), topicId);
                return false;
            }
        }

        // Subscriber not found
        log.warn("Subscriber {} not found during unsubscription from Topic {}", subscriber.getName(), topicId);
        return false;
    }

    private void startHeartbeatCheck(Subscriber subscriber) {
        Timer timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    Registry registry = LocateRegistry.getRegistry(subscriber.getHost(), subscriber.getPort());
                    SubscriberInterface subscriberStub = (SubscriberInterface) registry.lookup(subscriber.getName());

                    // check
                    if (subscriberStub.isAlive()) {
                        log.info("Subscriber {} is alive", subscriber.getName());
                    } else {
                        log.warn("Subscriber {} did not respond to heartbeat, removing...", subscriber.getName());
                        handleSubscriberCrash(subscriber);
                        timer.cancel();
                    }

                } catch (Exception e) {
                    log.error("Failed to reach subscriber {} during heartbeat check, removing...", subscriber.getName());
                    handleSubscriberCrash(subscriber);
                    timer.cancel();
                }
            }
        }, 0, heartbeatInterval);
    }
    private void handleSubscriberCrash(Subscriber subscriber){
        // remove all subscriptions
        for (Topic topic : subscriber.getTopics()) {
            try {
                unsubscribeFromTopic(subscriber, topic.getTopicId());
            } catch (RemoteException e) {
                log.error("Failed to unsubscribe subscriber {} from topic {}", subscriber.getName(), topic.getTopicId(), e);
            }
        }
        connectedSubscribers.remove(subscriber);
        log.info("Subscriber {} and its subscriptions have been removed.", subscriber.getName());
    }

    // Register with Directory Service
    public void registerWithDirectoryService(String directoryHost, int directoryPort) {
        try {
            Registry registry = LocateRegistry.getRegistry(directoryHost, directoryPort);
            DirectoryService directoryService = (DirectoryService) registry.lookup("DirectoryService");
            directoryService.registerBroker(this.broker);
            List<Broker> registeredBrokers = directoryService.getRegisteredBrokers();
            log.info("Successfully registered with Directory Service. Current brokers: {}", registeredBrokers);
            for (Broker broker : registeredBrokers) {
                if (!broker.getName().equals(this.broker.getName())) {
                    newBrokerRegistered(broker);
                }
            }
        } catch (Exception e) {
            log.error("Failed to register with Directory Service", e);
        }
    }
}

