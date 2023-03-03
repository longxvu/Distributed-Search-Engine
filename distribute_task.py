import socket

# Define the IP addresses and port numbers of the worker nodes
WORKER_IPS = ['128.195.42.102']
WORKER_PORTS = [5852]

def master():
    # Create sockets and connect to each worker node
    workers = []
    print('test from 10')
    for i in range(len(WORKER_IPS)):
        print(len(WORKER_IPS))
        print('test from 12')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print('test from 14')
        s.connect((WORKER_IPS[i], WORKER_PORTS[i]))
        print('test from 16')
        workers.append(s)
        print('Master connected to worker nodes', i)
    

    tasks = [1, 2, 3, 4, 5] # Define tasks to be performed
    for task in tasks:
        # Send the task to the next available worker node
        worker = workers.pop(0)
        worker.sendall(str(task).encode())
        workers.append(worker)
        print(f"Master sent task {task} to worker {worker.getpeername()}")

    # Send a signal to each worker node to stop waiting for tasks
    for worker in workers:
        worker.sendall(b'')
        worker.close()

if __name__ == '__main__':
    master()
