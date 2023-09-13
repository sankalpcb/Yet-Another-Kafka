from socket import * 
import sys
from producer_topic_util import Topic_prod
from consumer_topic_util import Topic_cons
import os,shutil
import threading
PWD= os.getcwd()
PARTITION_THRESHOLD = 10



serverSocket = socket(AF_INET,SOCK_STREAM) 
serverSocket.bind(('localhost' ,10003)) 
serverSocket.listen(10) 
print('[BROKER 3 RUNNING]') 

clientSocket = socket(AF_INET, SOCK_STREAM) 
clientSocket.connect(('localhost' , 13000))
clientSocket.send('im alive'.encode())
leader=clientSocket.recv(1024).decode() 
print(leader)

try:
    os.mkdir(PWD+'/broker2')
except Exception as e:
    shutil.rmtree(PWD+'/broker2')
    os.mkdir(PWD+'/broker2')



f=open(PWD+'/broker2/'+'log.txt','w')
f.close()
clientSocket.send('im alive'.encode())





while True: 
    connectionSocket, addr = serverSocket.accept() 
    sentence = connectionSocket.recv(1024).decode()


    if sentence =='producer':
        



        print('[PRODUCER CONNECTED]')
        connectionSocket.send('inside add data branch'.encode())
        id=connectionSocket.recv(1024).decode() 
        connectionSocket.send('id recieved : '.encode())
        topic_name=connectionSocket.recv(1024).decode() 
        connectionSocket.send('topic_name recieved : '.encode())
        
        while True:
            content=connectionSocket.recv(1024).decode() 
            if content == 'DISCONNECT DISCONNECT DISCONNECT':
                break
            print(content)
            connectionSocket.send('content recieved : '.encode())
            new_write=Topic_prod(topic_name)
            new_write.produce(id,content,PARTITION_THRESHOLD,leader)
        retSentence = 'Done'
        connectionSocket.send(retSentence.encode()) 
    
    elif sentence=='consumer':
        connectionSocket.send('inside conumer branch'.encode())
        sentence = connectionSocket.recv(1024).decode()
        if sentence =='Consume Data':
            print('[CONSUMER CONNECTED]')

            id=connectionSocket.recv(1024).decode() 
            connectionSocket.send('id recieved '.encode())
            
            topic_name=connectionSocket.recv(1024).decode() 
            connectionSocket.send('topic_name recieved '.encode())

            beg=connectionSocket.recv(1024).decode() 
            connectionSocket.send('beg recieved '.encode())

            new_read=Topic_cons(id)

            while True:
                
                try:
                    ret=''
                    ret = new_read.consume(topic_name,beg)
                    
                    
                    while ret!='':
                        connectionSocket.send(ret.encode()) 
                        rcv=connectionSocket.recv(1024).decode()
                    
                    

                except Exception as e:
                    pass


    connectionSocket.close()