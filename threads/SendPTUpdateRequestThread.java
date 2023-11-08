package threads;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;


public class SendPTUpdateRequestThread implements Runnable {
    String key;
    Integer peer_host_id, self_host_id;
    InetAddress peer_address;
    int peer_port;
    DatagramSocket socket;

    public SendPTUpdateRequestThread(Integer peer_host_id, String peer_address, String key, Integer self_host_id) throws UnknownHostException, SocketException{
        this.key = key;
        this.peer_host_id = peer_host_id;
        this.self_host_id = self_host_id;

        socket = new DatagramSocket();
        this.peer_address = InetAddress.getByName(peer_address);
        this.peer_port = 10000 + this.peer_host_id;
    }

    public String sendRequest(String req) {
        try {
            // Send PTUPDATE request
            byte[] buf = req.getBytes();
            DatagramPacket packet = new DatagramPacket(buf, buf.length, this.peer_address, this.peer_port);
            this.socket.send(packet);

            System.out.println("REQ Sent: " + req + " To host_id: " + this.peer_host_id);
            
            // Receive ack from peers
            DatagramPacket ack_packet = new DatagramPacket(buf, buf.length);
            socket.receive(ack_packet);
            String received = new String(ack_packet.getData(), 0, ack_packet.getLength());
            System.out.println("Recvd: " + received + " from host " + this.peer_host_id);

            return received;
        } catch (IOException e) {
            // TODO: handle exception
            System.out.println(e);
            return "";
        }
        
    }

    public void run(){
        String request = "PTUPDATE "+key+" "+this.self_host_id+"\n";
        try{
            String response = this.sendRequest(request);
            response = response.trim();
            if(!response.equals("EXECUTED")){
                System.out.println("FAILED TO EXECUTE PTUPDATE for host "+this.peer_host_id);
            }
        } catch(Exception e){}
    }
}
