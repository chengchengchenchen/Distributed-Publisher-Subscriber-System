package Broker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import Utils.*;


// Broker Interface
public interface BrokerInterface extends Remote {
    // Broker Service
    void newBrokerRegistered(Broker broker) throws RemoteException;

    // Publisher Service
    void newPublisherConnected(Publisher publisher) throws RemoteException;
    boolean createTopic(Publisher publisher, Topic topic) throws RemoteException;
    Map<Topic, Integer> getTopicDetails(Publisher publisher, List<String> processedBrokers) throws RemoteException;
    void publishMessage(Publisher publisher, Message message, List<String> processedBrokers) throws RemoteException;
    boolean deleteTopic(Publisher publisher, Topic topic, List<String> processedBrokers) throws RemoteException;

    // Subscriber Service
    void newSubscriberConnected(Subscriber subscriber) throws RemoteException;
    Map<Topic, String> listAllTopics(List<String> processedBrokers) throws RemoteException;
    boolean subscribeToTopic(Subscriber subscriber, String topicId) throws RemoteException;
    Set<Topic> getSubscribedTopics(Subscriber subscriber) throws RemoteException;
    boolean unsubscribeFromTopic(Subscriber subscriber, String topicId) throws RemoteException;


}

