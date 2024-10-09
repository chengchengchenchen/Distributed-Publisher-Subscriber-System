package Subscriber;

import Utils.Message;
import Utils.Topic;
import lombok.extern.slf4j.Slf4j;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
@Slf4j
public class SubscriberImpl extends UnicastRemoteObject implements SubscriberInterface {
    protected SubscriberImpl() throws RemoteException {
        super();
    }

    // Heartbeat
    @Override
    public boolean isAlive() throws RemoteException{
        log.info("Subscriber is alive.");
        return true;
    }

    @Override
    public void notifyTopicDeleted(Topic topic) throws RemoteException{
        log.info("Topic deleted: " + topic);
    }

    @Override
    public void receiveMessage(Message message) throws RemoteException {
        log.info(message.getFormattedMessage());
    }
}
