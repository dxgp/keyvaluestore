package threads;

import java.net.Socket;
import java.util.Map;

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
            String final_data = "";
            for (Map.Entry<String,String> entry : kv_store.local_store.entrySet()) {
                final_data = final_data + entry.getKey() + " " + entry.getValue() + "|";
            } 
            final_data = final_data + "\n";
            // dos.writeBytes(final_data);
            socket.getOutputStream().write(final_data.getBytes());
            socket.getOutputStream().flush();
        } catch(Exception e){e.printStackTrace();}
    }
}
