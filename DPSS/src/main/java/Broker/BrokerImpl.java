package Broker;

import Directory.DirectoryService;
import Publisher.PublisherInterface;
import Utils.Broker;
import Utils.Publisher;
import Utils.Subscriber;
import Utils.Topic;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

// Broker Implementation
@Slf4j
class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private final Broker broker;
    private final List<Subscriber> connectedSubscribers;
    private final List<Publisher> connectedPublishers;
    private final List<Broker> connectedBrokers;
    private final long heartbeatInterval = 5000L; // heartbeat per 5 sec
    protected BrokerImpl(Broker broker) throws RemoteException {
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
            try{
                Registry registry = LocateRegistry.getRegistry(broker.getHost(), broker.getPort());
                BrokerInterface brokerStub = (BrokerInterface) registry.lookup(broker.getName());
                brokerStub.printTest("aaa");
            }catch (Exception e){
                log.error("Test", e);
            }

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
                        connectedPublishers.remove(publisher);
                        timer.cancel();
                    }

                } catch (Exception e) {
                    log.error("Failed to reach publisher {} during heartbeat check, removing...", publisher.getName(), e);
                    connectedPublishers.remove(publisher);
                    timer.cancel();
                }
            }
        }, 0, heartbeatInterval);
    }

    // Publisher Create Topic
    @Override
    public void createTopic(Publisher publisher, Topic topic) throws RemoteException {
        log.info("Publisher {} is creating topic {}", publisher.getName(), topic.getName());

        for (Publisher connectedPublisher : connectedPublishers) {
            if (connectedPublisher.getName().equals(publisher.getName()) &&
                    connectedPublisher.getHost().equals(publisher.getHost()) &&
                    connectedPublisher.getPort() == publisher.getPort()) {

                // update topic
                connectedPublisher.addTopic(topic);

                log.info("Topic {} created by Publisher {} in Broker", topic.getName(), connectedPublisher.getName());
                return;
            }
        }

        log.warn("Publisher {} not found in connected publishers", publisher.getName());
    }

    @Override
    public void printTest(String input) throws RemoteException {
        log.info(input);
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

