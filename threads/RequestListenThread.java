package threads;

import storage.KeyValueStore;

import java.net.ServerSocket;
import java.net.Socket;

/*
 * A thread to listen to incoming requests. Hands the request over to RequestHandlerThread for parsing
 * and then returns to listening.
 */
public class RequestListenThread implements Runnable{
    int host_id;
    KeyValueStore kv_store;
    ServerSocket accept_socket;
    Socket socket;
    public RequestListenThread(int host_id,KeyValueStore kv_store){
        try{
            this.host_id = host_id;
            this.kv_store = kv_store;
            accept_socket = new ServerSocket(10000+host_id);
        } catch(Exception e){e.printStackTrace();}
    }
    public void run(){
        System.out.println("Now listening for incoming connections");
        try{            
            while(true){
                socket = accept_socket.accept();
                new Thread(new RequestHandlerThread(socket, kv_store)).start();;
            }
        } catch(Exception e){e.printStackTrace();}
    }
}
