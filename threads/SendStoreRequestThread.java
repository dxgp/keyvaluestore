package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

public class SendStoreRequestThread implements Runnable{
    Integer host_id;
    ConcurrentHashMap<String,String> total_map;
    InetAddress address;
    DatagramSocket socket;
    int port;

    public SendStoreRequestThread(Integer host_id, String address, ConcurrentHashMap<String,String> total_map) throws UnknownHostException, SocketException{
        this.host_id = host_id;
        this.address = InetAddress.getByName(address);
        this.total_map = total_map;
        this.port = 10000 + host_id;
        this.socket = new DatagramSocket();
    }

    public String sendRequest(String req) {
        try {
            // Send STORE request
            byte[] buf = req.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, this.address, this.port);
            this.socket.send(packet);

            System.out.println("REQ Sent: " + req + " To host_id: " + (this.port-10000));
            
            // Receive ack from peers
            byte[] ack_buf = new byte[2000];
            DatagramPacket ack_packet = new DatagramPacket(ack_buf, ack_buf.length);
            socket.receive(ack_packet);
            String received = new String(ack_packet.getData(), 0, ack_packet.getLength());
            System.out.println("Recvd: " + received + " from host " + (this.port-10000));
            return received;
        } catch (IOException e) {
            // TODO: handle exception
            System.out.println(e);
            return "";
        }
        
    }

    public void run(){
        String request = "STORE";
        try{

            String response = this.sendRequest(request);

            ConcurrentHashMap<String,String> recvd_hm = new ConcurrentHashMap<String,String>();

            String[] rows = response.split("\\|");

            for(int i=0;i<rows.length;i++){
                String[] kv = rows[i].split("\\ ");
                recvd_hm.put(kv[0], kv[1]);
            }

            total_map.putAll(recvd_hm);

            System.out.println("RECVD HM FROM HOST " + this.host_id + ": " + recvd_hm.toString());

        } catch(Exception e){}
    }
}
