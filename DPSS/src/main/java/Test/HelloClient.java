package Test;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class HelloClient {
    public static void main(String[] args) {
        try {
            // 获取本地的 RMI 注册表
            Registry registry = LocateRegistry.getRegistry("localhost", 1099);

            // 查找远程对象
            HelloInterface hello = (HelloInterface) registry.lookup("HelloService");

            // 调用远程方法
            String response = hello.sayHello("Changmei");
            System.out.println("远程调用结果: " + response);
            new Thread(() -> {
                while (true) {
                    try {
                        // 每隔5秒进行一次心跳检测
                        Thread.sleep(5000);
                        hello.heartbeat();
                        System.out.println("Publisher is alive.");
                    } catch (RemoteException e) {
                        System.err.println("Publisher has disconnected.");
                        break; // 停止心跳检测
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
