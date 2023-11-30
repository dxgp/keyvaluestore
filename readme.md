



# Compilation instructions:
1. cd into the required directory (tcp, udp, rmi)
2. Compile files using 
```bash 
javac Program.java
```

# Running instructions:
- If running the program on multiple physical hosts, replace localhost in the below commands to that host's ip address. 
- For running on same host use localhost.

## For UDP:     
Running command syntax (For 2 nodes. Can be extended similarly to n nodes):    

```bash
java Program <current_node_id> <total_node_count> <node_1_id>:<node_1_ip_address> <node_2_id>:<node_2_ip_address>
```

### Example commands for running 2, 3, 4 nodes:
- For running 2 nodes run these commands in 2 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 2 0:localhost 1:localhost
    ```


    - For node 1:    
    ```bash
    java Program 1 2 0:localhost 1:localhost 
    ```

- Similarly For running 3 nodes run these commands in 3 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 3 0:localhost 1:localhost 2:localhost
    ```


    - For node 1:    
    ```bash
    java Program 1 3 0:localhost 1:localhost 2:localhost
    ```

    - For node 2:   
    ```bash
    java Program 2 3 0:localhost 1:localhost 2:localhost
    ``````


- For running 4 nodes run these commands in 4 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 4 0:localhost 1:localhost 2:localhost 3:localhost
    ```


    - For node 1:    
    ```bash
    java Program 1 4 0:localhost 1:localhost 2:localhost 3:localhost
    ```

    - For node 2:   
    ```bash
    java Program 2 4 0:localhost 1:localhost 2:localhost 3:localhost
    ``````

    - For node 3:   
    ```bash
    java Program 3 4 0:localhost 1:localhost 2:localhost 3:localhost
    ```


## For TCP:   
- If running on multiple physical hosts use following command:    
- On host 0:
```bash
java Program 0 2 <host_0_ip_address> <host_1_ip_address>
```
- On host 1:
```bash
java Program 1 2 <host_1_ip_address> <host_0_ip_address>
```
Running command syntax (For 2 nodes. Can be extended similarly to n nodes):    

```bash
java Program <current_node_id> <total_node_count> <node_1_ip_address> <node_2_ip_address>
```

### Example commands for running 2, 3, 4 nodes:
- For running 2 nodes run these commands in 2 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 2
    ```


    - For node 1:    
    ```bash
    java Program 1 2 
    ```

- Similarly For running 3 nodes run these commands in 3 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 3
    ```


    - For node 1:    
    ```bash
    java Program 1 3
    ```

    - For node 2:   
    ```bash
    java Program 2 3
    ``````


- For running 4 nodes run these commands in 4 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 4
    ```

    - For node 1:    
    ```bash
    java Program 1 4 
    ```

    - For node 2:   
    ```bash
    java Program 2 4 
    ``````

    - For node 3:   
    ```bash
    java Program 3 4 
    ```


## For RMI:    
- If running on multiple physical hosts use following command:   
- On host 0:
```bash
java Program 0 2 <host_0_ip_address> <host_1_ip_address>
```
- On host 1:
```bash
java Program 1 2 <host_1_ip_address> <host_0_ip_address>
```

Running command syntax (For 2 nodes. Can be extended similarly to n nodes):    

```bash
java Program <current_node_id> <total_node_count> <node_1_ip_address> <node_2_ip_address>
```

### Example commands for running 2, 3, 4 nodes:
- For running 2 nodes run these commands in 2 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 2 
    ```


    - For node 1:    
    ```bash
    java Program 1 2  
    ```

- Similarly For running 3 nodes run these commands in 3 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 3 
    ```


    - For node 1:    
    ```bash
    java Program 1 3 
    ```

    - For node 2:   
    ```bash
    java Program 2 3 
    ``````


- For running 4 nodes run these commands in 4 different terminals:   
    - For node 0:   
    ```bash
    java Program 0 4
    ```


    - For node 1:    
    ```bash
    java Program 1 4
    ```

    - For node 2:   
    ```bash
    java Program 2 4
    ``````

    - For node 3:   
    ```bash
    java Program 3 4 
    ```