package threads;
import java.net.*;
import java.io.*;
import java.util.*;
import storage.KeyValueStore;

public class RequestHandlerThread implements Runnable{
    private final Socket socket;
    BufferedReader server_in;
    DataOutputStream server_out;
    KeyValueStore kv_store;
    public RequestHandlerThread(Socket socket,KeyValueStore kv_store){
        this.socket = socket;
        this.kv_store = kv_store;
    }
    public void run(){
        try{
            server_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            server_out = new DataOutputStream(new DataOutputStream(socket.getOutputStream()));
            while(true){ // all requests from this host will be handled by this thread from now on.
                while(!server_in.ready());
                //char[] buf = new char[100];
                String buf = server_in.readLine();
                String client_query = new String(buf);
                String[] queryTerms = this.kv_store.parseRequest(client_query);
                System.out.println("Request received"+Arrays.toString(queryTerms));
                String resp = kv_store.process_query(queryTerms, kv_store);
                server_out.writeBytes(resp+"\n");
                server_out.flush();
            }
        } catch(IOException e){
            System.out.println(e.toString());
        }
    }
}
