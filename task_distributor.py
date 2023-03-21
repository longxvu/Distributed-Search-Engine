import multiprocessing
import socket
from multiprocessing import Process, Pool, Queue
import random
import time
import pickle
from utils import parse_config
from retriever import Retriever
from collections import deque

# get the hostname
host = socket.gethostname()  # localhost for now
port = 5000  # initiate port no above 1024
RECEIVE_MESSAGE_BUFFER_SIZE = 4096
NUM_WORKERS = 2
TERM_DISTRIBUTED = True
USE_CACHE = True
MAX_CACHE_SIZE = 150
PRINT_MESSAGE_INFO = True

def get_input_list(file_path):
    with open(file_path) as f:
        data = [line.strip() for line in f.readlines()]

    return data

def new_worker_task(conn, address, query, query_idx, term_distributed):
    # Values for term distributed
    if term_distributed:
        conn.send("1".encode())
    else:
        conn.send("2".encode())

    # Send the term or query
    conn.send(query.encode())  # send data to the client

    # Receive size of dict first, and actual content of dict second
    receiving_message_size = int(conn.recv(1024).decode())
    result = conn.recv(receiving_message_size)
    result = pickle.loads(result)

    # if PRINT_MESSAGE_INFO:
    #     print(f"Message from {address}: {result}")

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

def distribute_task(query_list, term_distributed=True):
    if term_distributed:
        query_list = [retriever.process_query(query) for query in query_lst]

    print(query_list)

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
        if term_distributed:
            for token in query:
                input_pool.put((token, query_idx))
        else:
            input_pool.put((query, query_idx))

    cache_result = {}
    cache_queue = deque()

    start_time = time.time()

    # Round-robin distributing each term to the process
    # machine idx
    worker_idx = 0
    while not input_pool.empty():
        token, query_idx = input_pool.get()

        # Get result from cache, update its position in queue
        if USE_CACHE:
            if token in cache_result:
                cache_queue.remove(token)
                cache_queue.append(token)
                result_lst[query_idx].append(cache_result[token])
                if PRINT_MESSAGE_INFO:
                    print(f"Found [{token}] in cache. Skipping")
                continue

        processed = False
        while not processed:
            # No task currently running
            if not async_results[worker_idx]:
                async_results[worker_idx] = workers_pool[worker_idx].apply_async(new_worker_task,
                                                                   (connections[worker_idx], addresses[worker_idx], token, query_idx, term_distributed))
                # Term is processed, let's skip to next term
                if PRINT_MESSAGE_INFO:
                    print(f"Sending [{token}] to worker {worker_idx} {addresses[worker_idx]}")
                processed = True
            else:
                # Worker is done, set results to None
                if async_results[worker_idx].ready():
                    # token_idx is same as query_idx (?)
                    result, token_idx = async_results[worker_idx].get()
                    result_lst[token_idx].append(result)
                    async_results[worker_idx] = None
                    # Update cache
                    if USE_CACHE:
                        if token not in cache_result:
                            cache_result[token] = result
                            cache_queue.append(token)
                        if len(cache_result) > MAX_CACHE_SIZE:
                            item = cache_queue.popleft()
                            del cache_result[item]

            worker_idx = (worker_idx + 1) % NUM_WORKERS

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

    # Merge all the result now
    doc_tf_idf_maps = []

    if term_distributed:
        for token_tf_idf_lst in result_lst:  # each result contains multiple tokens
            doc_tf_idf_map = {}
            for tokens_tf_idf in token_tf_idf_lst:  # for each token in token tf idf list
                for doc_id in tokens_tf_idf:  # doc id for each token
                    if doc_id not in doc_tf_idf_map:
                        doc_tf_idf_map[doc_id] = tokens_tf_idf[doc_id]
                    else:
                        doc_tf_idf_map[doc_id] += tokens_tf_idf[doc_id]
            doc_tf_idf_maps.append(doc_tf_idf_map)
    else:
        doc_tf_idf_maps = [result[0] if len(result[0]) > 0 else {} for result in result_lst]

    end_time = time.time()

    # Closing the pool workers pool
    for pool in workers_pool:
        pool.close()
        pool.join()

    # Closing server connections
    server_socket.close()

    # Sorting and returning top k
    for idx in range(len(doc_tf_idf_maps)):
        doc_tf_idf_maps[idx] = sorted(doc_tf_idf_maps[idx].items(), key=lambda x: x[1], reverse=True)
        doc_tf_idf_maps[idx] = doc_tf_idf_maps[idx][:5]

    # doc_id[0] because doc_id is a tuple of (doc_id, tf_idf_score)
    doc_id_results = [[retriever.doc_id_url_map[doc_id[0]] for doc_id in doc_tf_idf_map] for doc_tf_idf_map in doc_tf_idf_maps]
    disk_loc_results = [[retriever.doc_id_disk_loc[doc_id[0]] for doc_id in doc_tf_idf_map] for doc_tf_idf_map in doc_tf_idf_maps]

    print("====================================")
    for idx in range(len(doc_id_results)):
        print(f"Query: {query_list[idx]}")
        for url, disk_loc in zip(doc_id_results[idx], disk_loc_results[idx]):
            print(url, disk_loc)
        print("====================================")

    print(f"Searching for {len(query_list)} queries took a total of {end_time - start_time:.3f}s")


if __name__ == '__main__':
    # Getting query input
    input_file_path = "query.txt"
    query_lst = get_input_list(input_file_path)

    default_config, data_config = parse_config()
    retriever = Retriever(data_config["indexer_state_dir"],
                        default_config["doc_id_file"],
                        default_config["all_posting_file"],
                        default_config["term_posting_map_file"])
    print("Retriever state loaded")
    # distribute all the task
    distribute_task(query_lst, TERM_DISTRIBUTED)
