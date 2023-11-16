package service;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;

public interface StorageService extends Remote{
    String put(String key,String value,int random_int) throws RemoteException;
    String get(String key) throws RemoteException;
    ConcurrentHashMap<String,String> store() throws RemoteException;
    String delete(String key) throws RemoteException;
    String test_call() throws RemoteException;
    void exit(Integer host_id) throws RemoteException;
    String ptupdate(String key,Integer host_id) throws RemoteException;
}