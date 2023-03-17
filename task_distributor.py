import multiprocessing
import socket
from multiprocessing import Process, Pool, Queue
import random
import time
import pickle
from utils import parse_config
from retriever import Retriever

# get the hostname
host = socket.gethostname()  # localhost for now
port = 5000  # initiate port no above 1024
RECEIVE_MESSAGE_BUFFER_SIZE = 4096
NUM_WORKERS = 1

def get_input_list(file_path):
    with open(file_path) as f:
        data = [line.strip() for line in f.readlines()]

    return data

def new_worker_task(conn, address, query, query_idx=0):
    # Send the term or query
    conn.send(query.encode())  # send data to the client

    # Receive size of dict first, and actual content of dict second
    receiving_message_size = int(conn.recv(1024).decode())
    result = conn.recv(receiving_message_size)
    result = pickle.loads(result)

    print(f"Message from {address}: {result}")
    return result, query_idx

def server_accept_new_worker(server_socket):
    conn, address = server_socket.accept()
    print("Connection from: " + str(address))
    return conn, address

def get_workers_connections(server_socket, num_workers=2):
    pool = multiprocessing.Pool(num_workers)

    # Need pool map since we want the main process blocked until everything connects
    pool_res = pool.map(server_accept_new_worker, [server_socket] * num_workers)
    connections, addresses = zip(*pool_res)
    return list(connections), list(addresses)

def distribute_task(query_list):
    server_socket = socket.socket()  # get instance
    # look closely. The bind() function takes tuple as argument
    server_socket.bind((host, port))  # bind host address and port together

    # configure how many client the server can listen simultaneously
    server_socket.listen(NUM_WORKERS)

    connections, addresses = get_workers_connections(server_socket, NUM_WORKERS)
    # print(connections)
    print(addresses)

    print(f"{NUM_WORKERS} workers connected")
    print("="*20)

    workers_pool = []
    async_results = [None] * NUM_WORKERS
    # Each worker has a separate pool of process
    for _ in range(NUM_WORKERS):
        workers_pool.append(Pool(1))

    input_pool = Queue()
    result_lst = [[] for _ in range(len(query_list))]

    # Put all input into a queue
    for query_idx, query in enumerate(query_list):
        for token in query:
            input_pool.put((token, query_idx))

    # Round-robin distributing each term to the process
    while not input_pool.empty():
        token, query_idx = input_pool.get()
        processed = False
        while not processed:
            for idx in range(NUM_WORKERS):
                # No task currently running
                if not async_results[idx]:
                    async_results[idx] = workers_pool[idx].apply_async(new_worker_task,
                                                                       (connections[idx], addresses[idx], token, query_idx))
                    # Term is processed, let's skip to next term
                    print(f"Sending [{token}] to {addresses[idx]}")
                    processed = True
                    break
                else:
                    # Worker is done, set results to None
                    if async_results[idx].ready():
                        result, token_idx = async_results[idx].get()
                        result_lst[token_idx].append(result)
                        async_results[idx] = None

    # Waiting for all results to complete running
    while True:
        all_complete = True
        for idx, async_result in enumerate(async_results):
            if async_result:
                if async_result.ready():
                    result, token_idx = async_result.get()
                    result_lst[token_idx].append(result)
                    async_results[idx] = None
                else:
                    all_complete = False
        if all_complete:
            break

    # Closing the pool workers pool
    for pool in workers_pool:
        pool.close()
        pool.join()

    # Merge all the result now
    doc_tf_idf_maps = []
    for token_tf_idf_lst in result_lst:  # each result contains multiple tokens
        doc_tf_idf_map = {}
        for tokens_tf_idf in token_tf_idf_lst:  # for each token in token tf idf list
            for doc_id in tokens_tf_idf:  # doc id for each token
                if doc_id not in doc_tf_idf_map:
                    doc_tf_idf_map[doc_id] = tokens_tf_idf[doc_id]
                else:
                    doc_tf_idf_map[doc_id] += tokens_tf_idf[doc_id]
        doc_tf_idf_maps.append(doc_tf_idf_map)

    # Closing server connections
    server_socket.close()

    # Sorting and returning top k
    for idx in range(len(doc_tf_idf_maps)):
        doc_tf_idf_maps[idx] = sorted(doc_tf_idf_maps[idx].items(), key=lambda x: x[1], reverse=True)
        doc_tf_idf_maps[idx] = doc_tf_idf_maps[idx][:5]

    # doc_id[0] because doc_id is a tuple of (doc_id, tf_idf_score)
    doc_id_results = [[retriever.doc_id_url_map[doc_id[0]] for doc_id in doc_tf_idf_map] for doc_tf_idf_map in doc_tf_idf_maps]
    disk_loc_results = [[retriever.doc_id_disk_loc[doc_id[0]] for doc_id in doc_tf_idf_map] for doc_tf_idf_map in doc_tf_idf_maps]

    for idx in range(len(doc_id_results)):
        print(f"Query: {query_list[idx]}")
        for url, disk_loc in zip(doc_id_results[idx], disk_loc_results[idx]):
            print(url, disk_loc)
        print("====================================")


if __name__ == '__main__':
    # Getting query input
    input_file_path = "query2.txt"
    query_lst = get_input_list(input_file_path)

    default_config, data_config = parse_config()
    retriever = Retriever(data_config["indexer_state_dir"],
                        default_config["doc_id_file"],
                        default_config["all_posting_file"],
                        default_config["term_posting_map_file"])
    print("Retriever state loaded")
    query_lst = [retriever.process_query(query) for query in query_lst]
    print(query_lst)
    # distribute all the task
    distribute_task(query_lst)
