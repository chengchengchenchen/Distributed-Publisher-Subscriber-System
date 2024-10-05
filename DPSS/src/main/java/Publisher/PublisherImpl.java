package Publisher;

import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
@Slf4j
public class PublisherImpl extends UnicastRemoteObject implements PublisherInterface {
    protected PublisherImpl() throws RemoteException {
        super();
    }

    // Heartbeat
    @Override
    public void heartbeat() throws RemoteException {
        log.info("Heartbeat sent from publisher.");
    }
}
