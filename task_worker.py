import socket
import time
import random
import sys
import pickle
from utils import parse_config
from retriever import Retriever

host = socket.gethostname()  # localhost
port = 5000  # socket server port number

def sleep_args():
    if len(sys.argv) >= 2:
        time.sleep(float(sys.argv[1]))

def receive_task(retriever):
    client_socket = socket.socket()  # instantiate
    client_socket.connect((host, port))  # connect to the server

    while True:
        term_distributed = client_socket.recv(1024).decode()
        if not term_distributed:
            break
        term_distributed = True if term_distributed == "1" else False
        data = client_socket.recv(1024).decode()
        print('Received from server: ' + data)  # show in terminal

        sleep_args()
        if not data:
            break

        if term_distributed:
            result = retriever.term_ranked_retrieval(data)
        else:
            result = retriever.retrieve(data)

        # print(result)
        sending_data = pickle.dumps(result)
        send_data_size = f"{len(sending_data)}".encode()

        # Send data size first, then sends data
        client_socket.send(send_data_size)
        client_socket.send(sending_data)

    client_socket.close()  # close the connection


if __name__ == '__main__':
    default_config, data_config = parse_config()
    retriever = Retriever(data_config["indexer_state_dir"],
                        default_config["doc_id_file"],
                        default_config["all_posting_file"],
                        default_config["term_posting_map_file"])
    print("Retriever state loaded")
    receive_task(retriever)