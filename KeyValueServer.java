import java.net.*;
import java.io.*;
import java.util.*;

public class KeyValueServer {
    private Map<String, String> data;
    KeyValueServer(){
        this.data = new HashMap<>();
    }
    public static void main(String[] args) throws IOException{
        String clientQuery;
        String result;
        KeyValueServer kv_server = new KeyValueServer();
        ServerSocket inSocket = new ServerSocket(9999);
        Socket connectionSocket = inSocket.accept();
        while(true){
            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(new DataOutputStream(connectionSocket.getOutputStream()));
            while(!inFromClient.ready());
            clientQuery = inFromClient.readLine();
            System.out.println(clientQuery);
            String[] queryTerms = clientQuery.split("\\|");
            System.out.println(Arrays.toString(queryTerms));
            result = kv_server.processQuery(queryTerms);
            outToClient.writeBytes(result);
            outToClient.flush();
        }
    }
    public String processQuery(String[] queryTerms){
        if(queryTerms[0].equals("GET")){
            System.out.println("GET QUERY RECEIVED");
            return data.get(queryTerms[1])+"\n";
        }
        else if(queryTerms[0].equals("PUT")){
            System.out.println("PUT QUERY RECEIVED");
            data.put(queryTerms[1],queryTerms[2]);
            return "AB\n";
        }
        else if(queryTerms[0].equals("DEL")){
            System.out.println("DELETE QUERY RECEIVED");
            data.remove(queryTerms[1]);
            return "AB\n";
        }
        else if(queryTerms[0].equals("STORE")){
            System.out.println("STORE QUERY RECEIVED");
            return "AB\n";
        }
        else{
            System.out.println("Invalid query provided");
            return "AB";
        }
    }

}
