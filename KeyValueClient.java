import java.net.*;
import java.io.*;

public class KeyValueClient {
    private Socket serverSocket;
    DataOutputStream outToServer;
    BufferedReader inFromServer;
    KeyValueClient(int server_port) throws UnknownHostException,IOException{
        this.serverSocket = new Socket("localhost",server_port);
        this.outToServer = new DataOutputStream(serverSocket.getOutputStream());
        this.inFromServer = new BufferedReader(new InputStreamReader(serverSocket.getInputStream()));
    }
    public static void main(String[] args) throws IOException{
        KeyValueClient client = new KeyValueClient(9999);
        Socket clientSocket = new Socket("localhost",9999);
        while(true){
            client.put("Gunjan","22");
            client.put("Shrini","26");
            client.put("Adwait","23");
            client.get("Shrini");
            client.del("Adwait");
        }
    }
    // TODO: Add JSON serialization here
    public void put(String key, String value) throws IOException{
        String query = "PUT"+ "|" +key + "|" + value + "\n";
        outToServer.writeBytes(query);
        while(!inFromServer.ready());
        String query_result = inFromServer.readLine();
        System.out.println(query_result);
        outToServer.flush();
    }
    public void get(String key) throws IOException{
        String query = "GET" + "|" + key + "\n";
        outToServer.writeBytes(query);
        while(!inFromServer.ready());
        String query_result = inFromServer.readLine();
        System.out.println(query_result);
        outToServer.flush();
    }
    public void del(String key) throws IOException{
        String query = "DEL" + "|" + key + "\n";
        outToServer.writeBytes(query);
        while(!inFromServer.ready());
        String query_result = inFromServer.readLine();
        System.out.println(query_result);
        outToServer.flush();
    }
    public void store() throws IOException{
        String query = "STORE" + "\n";
        outToServer.writeBytes(query);
        while(!inFromServer.ready());
        String query_result = inFromServer.readLine();
        System.out.println(query_result);
        outToServer.flush();
    }
}
