package threads;

import storage.KeyValueStore;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;

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
            System.out.println("BEFORE ACCEPT");
            System.out.println("AFTER ACCEPT");
            //BufferedReader sender_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            
            while(true){
                socket = accept_socket.accept();
                new Thread(new RequestHandlerThread(socket, kv_store)).start();;
            //     char buf = '\0';
            //     String query = "";
            //     while(!(buf == '\n')){
            //         buf = (char) socket.getInputStream().read();
            //         query += buf;
            //     }
            //     String[] query_terms = query.trim().split("\\ ");
            //     System.out.println("QUERY RECEIVED:"+Arrays.toString(query_terms));
            //     if(query_terms[0].equals("PUT")){
            //         String key = query_terms[1];
            //         String value = query_terms[2];
            //         int recvd_rand = Integer.parseInt(query_terms[3]);
            //         kv_store.handle_put(key, value, recvd_rand, socket);
            //     } else if(query_terms[0].equals("GET")){
            //         String key = query_terms[1];
            //         kv_store.handle_get(key, socket);
            //     } else if(query_terms[0].equals("STORE")){
            //         kv_store.handle_store(socket);
            //     } else if(query_terms[0].equals("DELETE")){
            //         String key = query_terms[1];
            //         kv_store.handle_delete(key, socket);
            //     } else if(query_terms[0].equals("PTUPDATE")){
            //         String key = query_terms[1];
            //         int host_id = Integer.parseInt(query_terms[2]);
            //         kv_store.handle_ptupdate(key, host_id, socket);
            //     } else{
            //         System.out.println("INVALID QUERY RECEIVED");
            //     }
            //     System.out.println("**RECEIVED QUERY PROCESSING FINISHED**");
            }
        } catch(Exception e){e.printStackTrace();}
    }
}
