package Directory;

import Broker.BrokerInterface;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

// Directory Service Implementation
@Slf4j
class DirectoryServiceImpl extends UnicastRemoteObject implements DirectoryService {
    private final List<String> brokerList;

    protected DirectoryServiceImpl() throws RemoteException {
        super();
        brokerList = new ArrayList<>();
    }

    @Override
    public synchronized void registerBroker(String brokerId, String brokerAddress) throws RemoteException {
        String brokerInfo = brokerId + "@" + brokerAddress;
        if (!brokerList.contains(brokerInfo)) {
            brokerList.add(brokerInfo);
            log.info("Broker registered: {}", brokerInfo);
            notifyExistingBrokers(brokerInfo);
        }
    }

    @Override
    public synchronized List<String> getRegisteredBrokers() throws RemoteException {
        return new ArrayList<>(brokerList);
    }

    // Notify existing brokers about the new broker
    private void notifyExistingBrokers(String newBrokerInfo) {
        for (String broker : brokerList) {
            if (!broker.equals(newBrokerInfo)) {
                try {
                    String[] brokerParts = broker.split("@");
                    String brokerAddress = brokerParts[1];
                    String host = brokerAddress.split(":")[0];
                    int port = Integer.parseInt(brokerAddress.split(":")[1]);
                    Registry registry = LocateRegistry.getRegistry(host, port);
                    BrokerInterface brokerStub = (BrokerInterface) registry.lookup(brokerParts[0]);
                    brokerStub.newBrokerRegistered(newBrokerInfo);
                    log.info("Notified broker {} about new broker: {}", broker, newBrokerInfo);
                } catch (Exception e) {
                    log.error("Failed to notify broker {} about new broker: {}", broker, newBrokerInfo, e);
                }
            }
        }
    }
}
