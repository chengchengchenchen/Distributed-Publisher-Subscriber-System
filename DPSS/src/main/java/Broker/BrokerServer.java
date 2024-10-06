package Broker;

import Utils.Broker;
import Utils.Constant;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

// Broker Server
@Slf4j
public class BrokerServer {
    public static void main(String[] args) {
        if (args.length < 2) {
            log.error("Usage: java BrokerServer <brokerName> <brokerAddress>");
            return;
        }

        String brokerName = args[0];
        String brokerAddress = args[1];
        try {
            String brokerHost = brokerAddress.split(":")[0];
            int brokerPort = Integer.parseInt(brokerAddress.split(":")[1]);

            BrokerImpl broker = new BrokerImpl(new Broker(brokerName, brokerHost, brokerPort));
            Registry registry = LocateRegistry.createRegistry(brokerPort);
            registry.rebind(brokerName, broker);
            log.info("Broker {} is running at {}", brokerName, brokerAddress);

            // Register with Directory Service
            broker.registerWithDirectoryService(Constant.DIRECTORY_HOST, Constant.DIRECTORY_PORT);
        } catch (RemoteException e) {
            log.error("Failed to start Broker", e);
        }
    }
}
