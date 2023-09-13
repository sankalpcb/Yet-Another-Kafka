from socket import * 
import sys
class Consumer:
    def __init__(self,id,topic_name,beg) -> None:
        self.id=id
        self.topic_name=topic_name
        self.beg=beg

    
    
    
    def data_consumer(self,clientSocket):
        sentence='Consume Data'
        clientSocket.send(sentence.encode()) 

        
        
        clientSocket.send(self.id.encode())
        
        rcv=clientSocket.recv(1024).decode() 
        

        
        
        clientSocket.send(self.topic_name.encode())
        
        rcv=clientSocket.recv(2048).decode() 
       

        
        
        clientSocket.send(self.beg.encode())
        
        rcv=clientSocket.recv(1024).decode() 
        
        
