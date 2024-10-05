package Broker;

import Directory.DirectoryService;
import Utils.Broker;
import Utils.Topic;
import Utils.Subscriber;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Broker Implementation
@Slf4j
class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private final String brokerName;
    private final String brokerAddress;
    private final Map<String, Subscriber> subscribers;
    private final List<Broker> connectedBrokers;

    protected BrokerImpl(String brokerName, String brokerAddress) throws RemoteException {
        super();
        this.brokerName = brokerName;
        this.brokerAddress = brokerAddress;
        this.subscribers = new HashMap<>();
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

    // Register with Directory Service
    public void registerWithDirectoryService(String directoryHost, int directoryPort) {
        try {
            Registry registry = LocateRegistry.getRegistry(directoryHost, directoryPort);
            DirectoryService directoryService = (DirectoryService) registry.lookup("DirectoryService");
            Broker thisBroker = new Broker(brokerName, brokerAddress.split(":")[0], Integer.parseInt(brokerAddress.split(":")[1]));
            directoryService.registerBroker(thisBroker);
            List<Broker> registeredBrokers = directoryService.getRegisteredBrokers();
            log.info("Successfully registered with Directory Service. Current brokers: {}", registeredBrokers);
            for (Broker broker : registeredBrokers) {
                if (!broker.getName().equals(brokerName)) {
                    newBrokerRegistered(broker);
                }
            }
        } catch (Exception e) {
            log.error("Failed to register with Directory Service", e);
        }
    }
}

