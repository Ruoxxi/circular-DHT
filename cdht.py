# use tcp protocol to insert and remove peers
# use udp protocol to send ping request and response messages
# set a tcpClient and a udpClient to send messages, a tcpServer and a udpServer to handle messages
# use three threads to run tcp, udp and ping

# Author: Ruoxi Fan


import socket
import os
import threading
import time
import sys

peer = -1
successor_first, successor_second = -1, -1  # the first and second successor
predecessor_first, predecessor_second = -1, -1  # the first and second predecessor
pingSeq_first, pingSeq_second = [], []  # record the sequence number that haven't receive ACK
running = True  # to decide whether the peer is running

host = "127.0.0.1"
basePort = 5000


# message format
class Message:
    def __init__(self):
        self.type = ""
        self.peer = -1
        self.data = ""
        self.seqNumber = -1
        self.successor = -1

    def send_ping_message(self, type, peer, seqNumber, successor):
        message = "{0}-{1}-{2}-{3}".format(type, peer, seqNumber, successor)
        return message.encode()

    def send_other_message(self, type, peer, data):
        message = "{0}-{1}-{2}".format(type, peer, data)
        return message.encode()

    def receive_message(self, message):
        m = message.decode()
        str = m.split("-")
        if len(str) == 3:
            self.type = str[0]
            self.peer = int(str[1])
            self.data = str[2]
        elif len(str) == 4:
            self.type = str[0]
            self.peer = int(str[1])
            self.seqNumber = int(str[2])
            self.successor = int(str[3])


def set_udp_port(port):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    address = (host, port)
    udp.bind(address)
    while running:
        data, _ = udp.recvfrom(1024)
        receive_udp_message(data, port - basePort)
    udp.close()


def set_tcp_port(port):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    address = (host, port)
    tcp.bind(address)
    tcp.listen(5)
    while running:
        client, _ = tcp.accept()
        data = client.recv(1024)
        receive_tcp_message(data, port - basePort)
        client.close()
    tcp.close()


def send_udp_message(data, address):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.sendto(data, address)
    udp.close()


def send_tcp_message(data, address):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.connect(address)
    tcp.send(data)
    tcp.close()


def receive_udp_message(data, peer):
    global pingSeq_first, pingSeq_second, predecessor_first, predecessor_second
    m = Message()
    m.receive_message(data)

    # receive ping request message
    if m.type == "pingRequest":
        print("A ping request was received from peer {}.".format(m.peer))
        message = m.send_ping_message("pingResponse", peer, m.seqNumber, m.successor)
        address = (host, basePort + m.peer)
        send_udp_message(message, address)
        if m.successor == 0:
            predecessor_first = m.peer
        elif m.successor == 1:
            predecessor_second = m.peer

    # receive ping response message
    elif m.type == "pingResponse":
        print("A ping response was received from peer {}".format(m.peer))
        if successor_first == m.peer and m.seqNumber in pingSeq_first:
            pingSeq_first.remove(m.seqNumber)
            pingSeq_first = [i for i in pingSeq_first if i > m.seqNumber]  # delete the early seqNumber
        elif successor_second == m.peer and m.seqNumber in pingSeq_second:
            pingSeq_second.remove(m.seqNumber)
            pingSeq_second = [i for i in pingSeq_second if i > m.seqNumber]


def receive_tcp_message(data, peer):
    global successor_first, successor_second, pingSeq_first, pingSeq_second, running
    m = Message()
    m.receive_message(data)
    # receive change-successor message
    if m.type == "quit":
        if m.data == "0":
            message = Message().send_other_message("successor", successor_first, 0)
            address = (host, basePort + m.peer)
            send_tcp_message(message, address)
        elif m.data == "1":
            message = Message().send_other_message("successor", successor_second, 1)
            address = (host, basePort + m.peer)
            send_tcp_message(message, address)
    # receive successor's successor
    if m.type == "successor":
        if m.data == "0":
            pingSeq_first = []
            successor_first = successor_second
            successor_second = m.peer
        elif m.data == "1":
            pingSeq_second = []
            successor_second = m.peer
        print("My first successor is now {}.".format(successor_first))
        print("My second successor is now {}.".format(successor_second))
    # get remove request
    if m.type == "removeRequest":
        remove_peer(peer)
        running = False
        print("I have been removed from the network!".format(m.peer))
    # remove a peer from network
    if m.type == "remove":
        successor = m.data.split("&")
        successor_first = int(successor[0])
        successor_second = int(successor[1])
        print("Peer {} was removed from the network.".format(m.peer))
        print("My first successor is now {}.".format(successor_first))
        print("My second successor is now {}.".format(successor_second))
    # find a place to insert a new peer
    if m.type == "find":
        find_peer(int(m.data), "insert", m.peer)
    # insert a peer into network
    if m.type == "insertRequest":
        successor = m.data.split("&")
        new_peer = int(successor[0])
        successor_first = int(successor[1])
        successor_second = int(successor[2])
        print("Peer {} has successfully insert!".format(new_peer))
        print("My first successor is now {}.".format(successor_first))
        print("My second successor is now {}.".format(successor_second))
    if m.type == "insertExist":
        print("Peer {} has already exist!".format(m.peer))


def ping(peer):
    global successor_first, successor_second, pingSeq_first, pingSeq_second
    request_seq_first = 0
    request_seq_second = 0
    while running:
        # send to successor first
        message = Message().send_ping_message("pingRequest", peer, request_seq_first, 0)
        address = (host, successor_first + basePort)
        send_udp_message(message, address)
        pingSeq_first.append(request_seq_first)
        request_seq_first += 1
        # send to successor second
        message = Message().send_ping_message("pingRequest", peer, request_seq_second, 1)
        address = (host, successor_second + basePort)
        send_udp_message(message, address)
        pingSeq_second.append(request_seq_second)
        request_seq_second += 1

        time.sleep(5)
        pingCheck(peer)


# check whether the peer is alive, if unreceived ping number more than 3, then the peer is no longer alive
def pingCheck(peer):
    global successor_first, successor_second, pingSeq_first, pingSeq_second
    if len(pingSeq_first) > 2:
        print("peer {} is not alive.".format(successor_first))
        message = Message().send_other_message("quit", peer, 0)
        address = (host, basePort + successor_second)
        send_tcp_message(message, address)
    elif len(pingSeq_second) > 2:
        print("peer {} is not alive.".format(successor_second))
        message = Message().send_other_message("quit", peer, 1)
        address = (host, basePort + successor_first)
        send_tcp_message(message, address)


def find_peer(peerNo, data, old_peer):
    # find the peer that request to be removed
    if data == "remove":
        message = Message().send_other_message("removeRequest", peer, " ")
        address = (host, basePort + peerNo)
        send_tcp_message(message, address)
    # find a property place to insert
    elif data == "insert":
        # find OK
        if peer < peerNo < successor_first or peerNo > peer > successor_first:
            insert_peer(peerNo)
        elif peer == peerNo:
            message = Message().send_other_message("insertExist", peer, " ")
            address = (host, basePort + old_peer)
            send_tcp_message(message, address)
        # find a place
        else:
            message = Message().send_other_message("find", old_peer, peerNo)
            address = (host, basePort + successor_first)
            send_tcp_message(message, address)


def initialize_new_peer(peerNo, new_successor_first, new_successor_second):
    # create the file
    initFile = "peer-{}.bat".format(peerNo)
    f = open(initFile, "w+")
    f.write("python cdht.py {0} {1} {2}".format(peerNo, new_successor_first, new_successor_second))
    f.close()
    startFile = "start-peer-{}.bat".format(peerNo)
    f = open(startFile, "w+")
    f.write("start {}".format(initFile))
    f.close()
    os.system(startFile)
    # delete the file
    time.sleep(4)
    if os.path.exists(initFile):
        os.remove(initFile)
    if os.path.exists(startFile):
        os.remove(startFile)


def insert_peer(peerNo):
    initialize_new_peer(peerNo, successor_first, successor_second)
    # notice peer
    message = Message().send_other_message("insertRequest", peer, "{0}&{1}&{2}".format(peerNo, peerNo, successor_first))
    address = (host, basePort + peer)
    send_tcp_message(message, address)
    # notice peer's predecessor
    message = Message().send_other_message("insertRequest", peer, "{0}&{1}&{2}".format(peerNo, peer, peerNo))
    address = (host, basePort + predecessor_first)
    send_tcp_message(message, address)


def remove_peer(peerNo):
    # change first predecessor's successor
    message = Message().send_other_message("remove", peer, "{0}&{1}".format(successor_first, successor_second))
    address = (host, basePort + predecessor_first)
    send_tcp_message(message, address)
    # change second predecessor's successor
    message = Message().send_other_message("remove", peer, "{0}&{1}".format(predecessor_first, successor_first))
    address = (host, basePort + predecessor_second)
    send_tcp_message(message, address)


def main():
    global successor_second, successor_first, running, peer
    # get peer and successors
    peer = int(sys.argv[1])
    successor_first = int(sys.argv[2])
    successor_second = int(sys.argv[3])
    print('Initialised Peer {}. Successor first is {}, successor second is {}'.format(peer, successor_first,
                                                                                      successor_second))
    # run three threads
    tcpThread = threading.Thread(target=set_tcp_port, args=(peer + basePort,))
    udpThread = threading.Thread(target=set_udp_port, args=(peer + basePort,))
    pingThread = threading.Thread(target=ping, args=(peer,))
    tcpThread.start()
    udpThread.start()
    pingThread.start()

    # Listen user input on terminal
    while running:
        user_input = input("Please input 'insert peer i' or 'remove peer i': \n")
        input_info = user_input.split(" ")
        # insert a peer
        if input_info[0] == "insert" and input_info[1] == "peer":
            new_peer = int(input_info[2])
            find_peer(new_peer, "insert", peer)
        # remove a peer
        elif input_info[0] == "remove" and input_info[1] == "peer":
            old_peer = int(input_info[2])
            find_peer(old_peer, "remove", peer)
            print("remove OK!")


if __name__ == "__main__":
    main()
