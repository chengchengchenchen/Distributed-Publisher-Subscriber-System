package Broker;

import Directory.DirectoryService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

// Broker Implementation
@Slf4j
class BrokerImpl extends UnicastRemoteObject implements BrokerInterface {
    private final String brokerId;
    private final String brokerAddress;

    protected BrokerImpl(String brokerId, String brokerAddress) throws RemoteException {
        super();
        this.brokerId = brokerId;
        this.brokerAddress = brokerAddress;
    }

    @Override
    public void newBrokerRegistered(String brokerInfo) throws RemoteException {
        log.info("New broker registered: {}", brokerInfo);
        // Implement logic to establish connection with the new broker if necessary
    }

    // Register with Directory Service
    public void registerWithDirectoryService(String directoryHost, int directoryPort) {
        try {
            Registry registry = LocateRegistry.getRegistry(directoryHost, directoryPort);
            DirectoryService directoryService = (DirectoryService) registry.lookup("DirectoryService");
            directoryService.registerBroker(brokerId, brokerAddress);
            List<String> registeredBrokers = directoryService.getRegisteredBrokers();
            log.info("Successfully registered with Directory Service. Current brokers: {}", registeredBrokers);
        } catch (Exception e) {
            log.error("Failed to register with Directory Service", e);
        }
    }
}
