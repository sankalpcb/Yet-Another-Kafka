from socket import * 
from consumerclass import Consumer
import sys
import json


serverName = 'localhost' 
serverPort = 10001

clientSocket = socket(AF_INET, SOCK_STREAM) 
clientSocket.connect((serverName , serverPort)) 


print('[CONNECTED]')


clientSocket.send('consumer'.encode()) 
recv=clientSocket.recv(1024).decode()

id=sys.argv[1]
topic=sys.argv[2]
if len(sys.argv) == 4: beg=sys.argv[3]
else: beg='--from-created'


consumer=Consumer(id,topic,beg)

consumer.data_consumer(clientSocket)



while True:
    
        retSentence = clientSocket.recv(1024) 
        clientSocket.send('content received'.encode())
        print('->', retSentence.decode()) 
    


 
clientSocket.close( )

