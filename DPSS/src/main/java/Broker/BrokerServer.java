package Broker;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

// Broker Server
@Slf4j
public class BrokerServer {
    public static void main(String[] args) {
        if (args.length < 3) {
            log.error("Usage: java BrokerServer <brokerId> <brokerAddress> <directoryHost> <directoryPort>");
            return;
        }

        String brokerId = args[0];
        String brokerAddress = args[1];
        String directoryHost = args[2];
        int directoryPort = Integer.parseInt(args[3]);

        try {
            BrokerImpl broker = new BrokerImpl(brokerId, brokerAddress);
            Registry registry = LocateRegistry.createRegistry(Integer.parseInt(brokerAddress.split(":")[1]));
            registry.rebind(brokerId, broker);
            log.info("Broker {} is running at {}", brokerId, brokerAddress);

            // Register with Directory Service
            broker.registerWithDirectoryService(directoryHost, directoryPort);
        } catch (RemoteException e) {
            log.error("Failed to start Broker", e);
        }
    }
}
