import java.net.*;
import java.io.*;
import java.util.*;
public class KeyValueStore {
    private Map<String,String> local_store;
    private Map<String, String> peer_table;
    // the structure of this map is: (id,(DataOutputStream,BufferedReader))
    // the DataOutputStream will be used for sending the data out while the buffered reader
    // will be used for data in.
    private Map<Integer,ArrayList<Object>> peers;
    int host_id;
    int total_hosts;
    int key_count;
    KeyValueStore(int host_id,int total_hosts,int key_count){
        this.local_store = new HashMap<>();
        this.peer_table = new HashMap<>();
        this.peers = new HashMap<>();
        this.key_count = key_count;
        this.total_hosts = total_hosts;
        this.host_id = host_id;
        initialize_peers();
    }

    public void initialize_peers(){
        for (int i = 0; i < this.total_hosts; i++) {
            if(i!=this.host_id){
                try{
                    Socket out_socket = new Socket("localhost",10000+i);
                    DataOutputStream out_stream = new DataOutputStream(out_socket.getOutputStream());
                    BufferedReader in_stream = new BufferedReader(new InputStreamReader(out_socket.getInputStream()));
                    ArrayList<Object> peer_streams = new ArrayList<Object>(Arrays.asList(out_stream, in_stream));
                    this.peers.put(i,peer_streams);
                } catch(Exception e){
                    System.out.println(e.toString());
                }
            }
        }
    }

    public static void main(String[] args) {
        /*command line arguments in the form:
          1. the id of this host
          2. total hosts
          3. total key count to get the range of random key generation
        */
        KeyValueStore kv_store = new KeyValueStore(Integer.parseInt(args[0]),Integer.parseInt(args[1]),Integer.parseInt(args[2]));
        RequestAcceptThread accept_thread = new RequestAcceptThread(kv_store.host_id);
        new Thread(accept_thread).start();
    }
    /*
     * A thread to accept incoming requests. Runs an infinite while loop and keeps accepting requests.
     * Once a request is accepted, it creates a new thread to handle it.
     */
    private static class RequestAcceptThread implements Runnable{
        ServerSocket accept_socket;
        public RequestAcceptThread(int host_id){
            try{
                accept_socket = new ServerSocket(10000+host_id);
            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
        public void run(){
            while(true){
                try{
                    Socket socket = accept_socket.accept();
                    // call the request handler thread here
                    RequestHandlerThread req_handler = new RequestHandlerThread(socket);
                    new Thread(req_handler).start();
                } catch(IOException e){
                    System.out.println(e.toString());
                }
            }
        }
    }
    private static class RequestHandlerThread implements Runnable{
        private final Socket socket;
        BufferedReader server_in;
        DataOutputStream server_out;
        public RequestHandlerThread(Socket socket){
            this.socket = socket;
        }
        public void run(){
            try{
                server_in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                server_out = new DataOutputStream(new DataOutputStream(socket.getOutputStream()));
                while(true){ // all requests from this host will be handled by this thread from now on.
                    while(!server_in.ready());
                    
                    // now process using the input here.
                    
                    server_out.flush();
                }

            } catch(IOException e){
                System.out.println(e.toString());
            }
        }
    }
}
