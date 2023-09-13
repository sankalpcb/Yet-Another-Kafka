from socket import * 
from producerclass import Producer
import sys

 





serverName = 'localhost'
serverPort = 10001



clientSocket = socket(AF_INET, SOCK_STREAM) 
clientSocket.connect((serverName , serverPort)) 




clientSocket.send('producer'.encode()) 



topic_name=sys.argv[1]
prod_id=sys.argv[2]


producer=Producer(prod_id,topic_name)
producer.data_producer(clientSocket)




while True:
    try:
        content=input('->')
        clientSocket.send(content.encode())
        rcv=clientSocket.recv(1024).decode()
    except EOFError as e:
        print(e)
        clientSocket.send('DISCONNECT DISCONNECT DISCONNECT'.encode())
        break





retSentence = clientSocket.recv(1024) 
print('[PRODUCE DATA]', retSentence.decode()) 



clientSocket.close( )
