import socket
import time
import random
import sys

host = socket.gethostname()  # localhost
port = 5000  # socket server port number

def receive_task():
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    # message = input(" -> ")  # take input

    # Receiving the data
    while True:
        data = client_socket.recv(1024).decode()
        # client_socket.send(message.encode())  # send message
        # receive response
        # sleep_duration = random.random()
        # time.sleep(sleep_duration)
        # print(f"Slept for {sleep_duration}")
        print('Received from server: ' + data)  # show in terminal
        time.sleep(float(sys.argv[1]))
        if not data:
            break

        client_socket.send(f"{data} {sys.argv[1]} {time.time()}".upper().encode())

    client_socket.close()  # close the connection


if __name__ == '__main__':
    receive_task()