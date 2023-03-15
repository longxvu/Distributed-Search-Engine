import multiprocessing
import socket
from multiprocessing import Process, Pool, Queue
import random
import time

# get the hostname
host = socket.gethostname()  # localhost for now
port = 5000  # initiate port no above 1024

def get_input_list(file_path):
    with open(file_path) as f:
        data = [line.strip() for line in f.readlines()]

    return data

def new_worker_task(conn, address, query, query_idx=0):
    # Client should wait for the data, and server should continuously send the data
    # print("Message from: " + str(address))
    # while True:
    #     # receive data stream. it won't accept data packet greater than 1024 bytes
    #     # data = conn.recv(1024).decode()
    #     # if not data:
    #         # if data is not received break
    #         # break
    conn.send(query.encode())  # send data to the client
    data = conn.recv(1024).decode()
        # if not data:
        #     break
    print(f"Message from {address}: {data}")

        # print("from connected user: " + str(data))
        # data = input(' -> ')
    # conn.close()  # close the connection
    return data, query_idx

def server_accept_new_worker(server_socket):
    conn, address = server_socket.accept()
    print("Connection from: " + str(address))
    return conn, address



def distribute_task(query_list, num_workers=2):
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(num_workers)

    # define a pool of workers
    pool = multiprocessing.Pool(num_workers)
    # connect to all workers before distributing task
    pool_res = pool.map(server_accept_new_worker, [server_socket] * num_workers)
    connections, address = zip(*pool_res)

    # conn, address = server_socket.accept()  # accept new connection
    # worker_connections.append((conn, address))

    print(f"{len(connections)} workers connected")
    print("="*20)

    # print(connections)
    # print(address)
    # query_list
    input_pool = []
    idx = 0
    for query_idx, query in enumerate(query_list):
        # random idx?
        # idx = random.randint(0, num_workers - 1)
        # print(idx)
        # input_pool.append((connections[idx], address[idx], query))
        for token in query.split():
            input_pool.append((connections[idx], address[idx], token, query_idx))
            idx = (idx + 1) % num_workers

    print(input_pool)

    pool.starmap_async(new_worker_task, input_pool)
    # start = time.time()
    # for inp in input_pool:
    #     pool.apply_async(new_worker_task, inp)
    # end = time.time()

    # print(f"Took {end - start:.3f}s")

    pool.close()
    pool.join()


    # while query list still have more stuffs?
    # for query in query_list:
    #     p = Process(target=new_worker_task, args=(conn, address, query,))
    #     p.start()
    #     p.join()
    #     print("One done")


if __name__ == '__main__':
    input_file_path = "query.txt"
    input_lst = get_input_list(input_file_path)
    distribute_task(input_lst)
