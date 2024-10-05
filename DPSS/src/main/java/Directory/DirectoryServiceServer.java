package Directory;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import Utils.Constant;
import lombok.extern.slf4j.Slf4j;

// Directory Service Server
@Slf4j
public class DirectoryServiceServer {

    public static void main(String[] args) {
        try {
            DirectoryService directoryService = new DirectoryServiceImpl();
            Registry registry = LocateRegistry.createRegistry(Constant.DIRECTORY_PORT);
            registry.rebind("DirectoryService", directoryService);
            log.info("Directory Service is running...");
        } catch (RemoteException e) {
            log.error("Failed to start Directory Service", e);
        }
    }
}

