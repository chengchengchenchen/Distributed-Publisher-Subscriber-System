package Broker;

import Utils.Constant;
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
            log.error("Usage: java BrokerServer <brokerName> <brokerAddress>");
            return;
        }

        String brokerName = args[0];
        String brokerAddress = args[1];
        String directoryHost = Constant.HOST;
        int directoryPort = Constant.DIRECTORY_PORT;

        try {
            BrokerImpl broker = new BrokerImpl(brokerName, brokerAddress);
            Registry registry = LocateRegistry.createRegistry(Integer.parseInt(brokerAddress.split(":")[1]));
            registry.rebind(brokerName, broker);
            log.info("Broker {} is running at {}", brokerName, brokerAddress);

            // Register with Directory Service
            broker.registerWithDirectoryService(directoryHost, directoryPort);
        } catch (RemoteException e) {
            log.error("Failed to start Broker", e);
        }
    }
}
