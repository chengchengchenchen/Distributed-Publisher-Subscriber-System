package Directory;

import Broker.BrokerInterface;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import Utils.Broker;
// Directory Service Implementation
@Slf4j
class DirectoryServiceImpl extends UnicastRemoteObject implements DirectoryService {
    private final List<Broker> brokers;

    protected DirectoryServiceImpl() throws RemoteException {
        super();
        this.brokers = new ArrayList<>();
    }

    @Override
    public synchronized void registerBroker(Broker broker) throws RemoteException {
        if (brokers.stream().noneMatch(b -> b.getName().equals(broker.getName()))) {
            brokers.add(broker);
            log.info("Broker registered: {}", broker);
        } else {
            log.warn("Broker with name {} is already registered", broker.getName());
        }
    }

    @Override
    public synchronized List<Broker> getRegisteredBrokers() throws RemoteException {
        return new ArrayList<>(brokers);
    }

    // Notify existing brokers about the new broker
    private void notifyExistingBrokers(Broker newBroker) {
        for (Broker broker : brokers) {
            if (!broker.getName().equals(newBroker.getName())) {
                try {
                    Registry registry = LocateRegistry.getRegistry(broker.getHost(), broker.getPort());
                    BrokerInterface brokerStub = (BrokerInterface) registry.lookup(broker.getName());
                    brokerStub.newBrokerRegistered(newBroker);
                    log.info("Notified broker {} about new broker: {}", broker, newBroker);
                } catch (Exception e) {
                    log.error("Failed to notify broker {} about new broker: {}", broker, newBroker, e);
                }
            }
        }
    }
}
