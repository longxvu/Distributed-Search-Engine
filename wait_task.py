import socket

# Define the IP address and port number of the master node
#'169.234.58.158', '169.234.25.170'
MASTER_IP = '169.234.58.158'
MASTER_PORT = 5555

def worker():
    # Create a socket and connect to the master node
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print("this is line 10")
        s.connect((MASTER_IP, MASTER_PORT))
        print('Worker connected to master node')

        while True:
            # Wait for a task to be received from the master node
            task = s.recv(1024).decode()
            if not task:
                break

            # Perform the task here
            print(f"Worker received task: {task}")
            data = "from machine 1 ****** \n"
            s.send(data.encode('utf-8'))

if __name__ == '__main__':
    worker()