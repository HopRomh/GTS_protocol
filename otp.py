import socket
import time

f = open("egts_packages.csv")
pacets = f.read().split('\n')[:-1]


for inter in pacets:
    soc = socket.socket()
    soc.connect(('localhost', 12349))
    soc.send(bytes.fromhex(inter))
    time.sleep(0.5)
    soc.close()

    

