/*
Student Name: Jingcheng Qian
Student ID: 1640690
*/
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
    private final long heartbeatInterval = 1000L; // heartbeat per 5 sec
    private final Set<String> processedRequests;
    public BrokerImpl(Broker broker) throws RemoteException {
        super();
        this.broker = broker;
        this.connectedPublishers = new ArrayList<>();
        this.connectedSubscribers = new ArrayList<>();
        this.connectedBrokers = new ArrayList<>();
        this.processedRequests = new HashSet<>();
    }

    @Override
    public synchronized void newBrokerRegistered(Broker broker) throws RemoteException {
        log.info("New broker registered: {}", broker.getName());
        if (connectedBrokers.stream().noneMatch(b -> b.getName().equals(broker.getName()))) {
            connectedBrokers.add(broker);
            log.info("Connected to new broker: {}", broker.getName());
        }
    }

    @Override
    public synchronized void newPublisherConnected(Publisher publisher) throws RemoteException {
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
                        //log.info("Publisher {} is alive", publisher.getName());
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
                deleteTopic(publisher, topic, System.currentTimeMillis() + publisher.getName() + topic.getTopicId());
            } catch (RemoteException e) {
                log.error("Failed to delete topic {} for publisher {}", topic.getTopicId(), publisher.getName(), e);
            }
        }

        connectedPublishers.remove(publisher);

        log.info("Publisher {} and all its topics have been removed", publisher.getName());
    }

    @Override
    public synchronized boolean createTopic(Publisher publisher, Topic topic) throws RemoteException {
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

    @Override
    public Map<Topic, Integer> getTopicDetails(Publisher publisher, String requestID) throws RemoteException {
        // check to avoid loop
        synchronized (processedRequests) {
            if (processedRequests.contains(requestID)) {
                log.info("Broker {} already processed this request {}", broker.getName(), requestID);
                return new HashMap<>();
            }
            processedRequests.add(requestID);
        }

        Map<Topic, Integer> topicDetails = new HashMap<>();

        // initialize
        synchronized (connectedPublishers) {
            for (Publisher connectedPublisher : connectedPublishers) {
                if (connectedPublisher.getName().equals(publisher.getName())) {
                    for (Topic topic : connectedPublisher.getTopics()) {
                        topicDetails.put(topic, 0);  // Initialize all Topic counts to 0
                    }
                    break;
                }
            }
        }

        synchronized (connectedSubscribers) {
            for (Subscriber subscriber : connectedSubscribers) {
                for (Topic topic : subscriber.getTopics()) {
                    if (topic.getPublisher().getName().equals(publisher.getName())) {
                        topicDetails.merge(topic, 1, Integer::sum);
                    }
                }
            }
        }

        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                Map<Topic, Integer> otherBrokerDetails = brokerStub.getTopicDetails(publisher, requestID);

                for (Map.Entry<Topic, Integer> entry : otherBrokerDetails.entrySet()) {
                    Topic topic = entry.getKey();
                    int count = entry.getValue();
                    // If the topic already exists, sum the subscriber counts. Otherwise, add it.
                    topicDetails.merge(topic, count, Integer::sum);
                }
            } catch (Exception e) {
                log.error("Failed to forward operation to broker {}", connectedBroker.getName(), e);
            }
        }

        return topicDetails;
    }

    @Override
    public void publishMessage(Publisher publisher, Message message, String requestID) throws RemoteException {
        // check to avoid loop
        synchronized (processedRequests) {
            if (processedRequests.contains(requestID)) {
                log.info("Broker {} already processed this request {}", broker.getName(), requestID);
                return;
            }
            processedRequests.add(requestID);
        }

        for (Subscriber subscriber : connectedSubscribers) {
            for (Topic topic : subscriber.getTopics()) {
                // check
                if (topic.getPublisher().getName().equals(publisher.getName()) && topic.getTopicId().equals(message.getTopic().getTopicId())) {
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
        }

        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                brokerStub.publishMessage(publisher, message, requestID);
            } catch (Exception e) {
                log.error("Failed to broadcast message to broker {}", connectedBroker.getName(), e);
            }
        }

        log.info("Message published for topic {}", message.getTopic().getTopicId());
    }

    // Publisher Delete Topic
    @Override
    public boolean deleteTopic(Publisher publisher, Topic topic, String requestID) throws RemoteException {
        // check to avoid loop
        synchronized (processedRequests) {
            if (processedRequests.contains(requestID)) {
                log.info("Broker {} already processed this request {}", broker.getName(), requestID);
                return false;
            }
            processedRequests.add(requestID);
        }

        log.info("Publisher {} is deleting topic {}", publisher.getName(), topic.getTopicId());

        // handle publisher
        synchronized (connectedPublishers) {
            for (Publisher connectedPublisher : connectedPublishers) {
                if (connectedPublisher.getName().equals(publisher.getName())) {
                    connectedPublisher.removeTopic(topic);
                    log.info("Topic {} removed from Publisher {}", topic.getTopicId(), publisher.getName());
                    break;
                }
            }
        }

        // handle subscriber
        Set<Subscriber> subscribersToUnsubscribe = new HashSet<>();
        synchronized (connectedSubscribers) {
            for (Subscriber subscriber : connectedSubscribers) {
                for (Topic subscribedTopic : subscriber.getTopics()) {

                    if (subscribedTopic.getTopicId().equals(topic.getTopicId())
                            && subscribedTopic.getPublisher().getName().equals(publisher.getName())) {
                        subscribersToUnsubscribe.add(subscriber);
                    }
                }
            }
        }
        for (Subscriber subscriber : subscribersToUnsubscribe) {
            subscriber.removeTopic(topic);
            log.info("Subscriber {} unsubscribed from topic id: {}", subscriber.getName(), topic.getTopicId());
            // notify
            try {
                Registry registry = LocateRegistry.getRegistry(subscriber.getHost(), subscriber.getPort());
                SubscriberInterface subscriberStub = (SubscriberInterface) registry.lookup(subscriber.getName());
                subscriberStub.notifyTopicDeleted(topic);
                log.info("Subscriber {} notified about topic id: {} deletion", subscriber.getName(), topic.getTopicId());
            } catch (Exception e) {
                log.error("Failed to notify subscriber {} about topic id: {} deletion", subscriber.getName(), topic.getTopicId(), e);
            }
        }


        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                brokerStub.deleteTopic(publisher, topic, requestID);
                log.info("Delete topic id: {} operation forwarded to broker {}", topic.getTopicId(), connectedBroker.getName());
            } catch (Exception e) {
                log.error("Failed to forward delete topic operation to broker {}", connectedBroker.getName(), e);
            }
        }
        return true;
    }

    @Override
    public synchronized void newSubscriberConnected(Subscriber subscriber) throws RemoteException {
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
    public Set<Topic> listAllTopics(String requestID) throws RemoteException {
        // check to avoid loop
        synchronized (processedRequests) {
            if (processedRequests.contains(requestID)) {
                log.info("Broker {} already processed this request {}", broker.getName(), requestID);
                return new HashSet<>();
            }
            processedRequests.add(requestID);
        }

        Set<Topic> allTopics = new HashSet<>();
        synchronized (connectedPublishers) {
            for (Publisher connectedPublisher : connectedPublishers) {
                allTopics.addAll(connectedPublisher.getTopics());
            }
        }

        // broadcast
        for (Broker connectedBroker : connectedBrokers) {
            try {
                Registry registry = LocateRegistry.getRegistry(connectedBroker.getHost(), connectedBroker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(connectedBroker.getName());
                Set<Topic> otherBrokerTopics = brokerStub.listAllTopics(requestID);
                allTopics.addAll(otherBrokerTopics);
            } catch (Exception e) {
                log.error("Failed to forward Broker {}", connectedBroker.getName(), e);
            }
        }

        return allTopics;
    }

    @Override
    public boolean subscribeToTopic(Subscriber subscriber, String topicId) throws RemoteException {
        // list all topics
        Set<Topic> allTopics = listAllTopics(System.currentTimeMillis() + broker.getName());

        /// Find topic
        Topic searchTopic = new Topic(topicId);
        Topic matchedTopic = allTopics.stream()
                .filter(topic -> topic.equals(searchTopic))
                .findFirst()
                .orElse(null);

        // check matchedTopic
        if (matchedTopic == null) {
            log.warn("Topic {} not found. Subscription failed for subscriber {}", topicId, subscriber.getName());
            return false;
        }

        // check topic is subscribed by this subscriber
        synchronized (connectedSubscribers){
            for (Subscriber existingSubscriber : connectedSubscribers) {
                if (existingSubscriber.getName().equals(subscriber.getName())) {
                    if (existingSubscriber.getTopics().contains(matchedTopic)) {
                        log.info("Subscriber {} is already subscribed to Topic {}", subscriber.getName(), matchedTopic.getTopicId());
                        return false;
                    } else {
                        // add
                        existingSubscriber.addTopic(matchedTopic);
                        log.info("Subscriber {} successfully subscribed to Topic {}", subscriber.getName(), matchedTopic.getTopicId());
                        return true;
                    }
                }
            }
        }

        // not found Subscriber
        log.warn("Subscriber {} not found in connectedSubscribers", subscriber.getName());
        return false;
    }

    @Override
    public Set<Topic> getSubscribedTopics(Subscriber subscriber) throws RemoteException {
        synchronized (connectedSubscribers) {
            Optional<Subscriber> existingSubscriber = connectedSubscribers.stream()
                    .filter(sub -> sub.getName().equals(subscriber.getName()))
                    .findFirst();

            if (existingSubscriber.isPresent()) {
                log.info("Subscriber {} is in this broker", subscriber.getName());
                return existingSubscriber.get().getTopics();
            } else {
                log.warn("Subscriber {} is not in this broker", subscriber.getName());
                return new HashSet<>();
            }
        }
    }

    @Override
    public boolean unsubscribeFromTopic(Subscriber subscriber, String topicId) throws RemoteException {
        synchronized (connectedSubscribers) {
            // find matched subscriber
            Optional<Subscriber> existingSubscriber = connectedSubscribers.stream()
                    .filter(sub -> sub.getName().equals(subscriber.getName()))
                    .findFirst();

            if (existingSubscriber.isPresent()) {
                Subscriber connectedSubscriber = existingSubscriber.get();

                // find matched topic
                Optional<Topic> subscribedTopic = connectedSubscriber.getTopics().stream()
                        .filter(topic -> topic.getTopicId().equals(topicId))
                        .findFirst();

                if (subscribedTopic.isPresent()) {
                    connectedSubscriber.removeTopic(subscribedTopic.get());
                    log.info("Subscriber {} successfully unsubscribed from Topic {}", subscriber.getName(), topicId);
                    return true;
                } else {
                    log.warn("Subscriber {} is not subscribed to Topic {}", subscriber.getName(), topicId);
                    return false;
                }
            } else {
                // not found
                log.warn("Subscriber {} not found during unsubscription from Topic {}", subscriber.getName(), topicId);
                return false;
            }
        }
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
                        //log.info("Subscriber {} is alive", subscriber.getName());
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

