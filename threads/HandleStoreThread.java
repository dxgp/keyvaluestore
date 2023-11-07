package threads;

import java.io.ObjectOutputStream;
import java.net.Socket;

import storage.KeyValueStore;

public class HandleStoreThread implements Runnable {
    KeyValueStore kv_store;
    Socket socket;
    public HandleStoreThread(KeyValueStore kv_store,Socket socket){
        this.kv_store = kv_store;
        this.socket = socket;
    }
    public void run(){
        try{
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(kv_store.local_store);
        } catch(Exception e){}
    }
}
