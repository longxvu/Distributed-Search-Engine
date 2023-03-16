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
    # queue.put((conn, address))
    return conn, address

def get_workers_connections(server_socket, num_workers=2):
    # processes = []
    # connections_queue = multiprocessing.Queue()

    # # Spawn processes to get connections
    # for _ in range(num_workers):
    #     p = multiprocessing.Process(target=server_accept_new_worker, args=(server_socket, connections_queue,))
    #     p.start()
    #     processes.append(p)
    # for p in processes:
    #     p.join()

    # connections = []
    # while not connections_queue.empty():
    #     connections.append(connections_queue.get())
    
    # Returning connection and addresses of connected workers
    pool = multiprocessing.Pool(num_workers)

    pool_res = pool.map(server_accept_new_worker, [server_socket] * num_workers)
    connections, addresses = zip(*pool_res)
    # connections, addresses = zip(*connections)
    return list(connections), list(addresses)



def distribute_task(query_list):
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    NUM_WORKERS = 2
    server_socket.listen(NUM_WORKERS)

    # define a pool of workers
    # pool = multiprocessing.Pool(num_workers)
    # connect to all workers before distributing task
    # pool_res = pool.map(server_accept_new_worker, [server_socket] * num_workers)
    # connections, address = zip(*pool_res)


    # conn, address = server_socket.accept()  # accept new connection
    # worker_connections.append((conn, address))

    connections, addresses = get_workers_connections(server_socket, NUM_WORKERS)
    print(connections)
    print(addresses)

    print(f"{NUM_WORKERS} workers connected")
    print("="*20)

    workers_pool = []
    async_results = [None] * NUM_WORKERS

    for _ in range(NUM_WORKERS):
        workers_pool.append(Pool(1))

    # print(connections)
    # print(address)
    # query_list
    input_pool = Queue()
    for query_idx, query in enumerate(query_list):
        # random idx?
        # idx = random.randint(0, num_workers - 1)
        # print(idx)
        # input_pool.append((connections[idx], address[idx], query))
        for token in query.split():
            input_pool.put((token, query_idx))

    while not input_pool.empty():
        token, query_idx = input_pool.get()
        processed = False
        while not processed:
            for idx in range(NUM_WORKERS):
                # No task currently running
                if not async_results[idx]:
                    async_results[idx] = workers_pool[idx].apply_async(new_worker_task, 
                                                                    (connections[idx], addresses[idx], token, query_idx))
                else:
                    # Worker is done, set results to None
                    if async_results[idx].ready():
                        result = async_results[idx].get()
                        # do something with the result here
                        async_results[idx] = None
                        processed = True
                    
    for pool in workers_pool:
        pool.close()
        pool.join()


    # print(input_pool)
    # exit()

    # pool.starmap_async(new_worker_task, input_pool)
    # start = time.time()
    # for inp in input_pool:
    #     pool.apply_async(new_worker_task, inp)
    # end = time.time()

    # print(f"Took {end - start:.3f}s")

    # pool.close()
    # pool.join()


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
