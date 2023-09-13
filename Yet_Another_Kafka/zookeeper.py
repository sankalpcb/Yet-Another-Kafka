import socket
import threading
import time

def countdown(t,conn,connected):
    while t:
        
        
        
        time.sleep(1)
        t -= 1
        
    conn.send('done'.encode())
    connected=False    
      
active_brokers=[3,2,1]
def leader_election():

    return active_brokers[-1]


def main():
    print(" Zookeeper is about to start...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost',13000))
    server.listen()
    

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        
        
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

def handle_client(conn, addr):
    

    connected = True
    print(connected)
    msg = conn.recv(1024).decode()
    conn.send(str(active_brokers[-1]).encode())
    print(msg)
    while connected:
        msg = conn.recv(1024).decode()
        if(msg=='im dead'):


            num=addr[1]/10000


            active_brokers.pop()
            LEADER=leader_election()
            conn.send(str(LEADER).encode())
            connected=False
                
    conn.close()



if __name__ == "__main__":
    main()