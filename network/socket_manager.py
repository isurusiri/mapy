import socket


class MapySocketManager(object):
    def __init__(self):
        host = socket.gethostbyname()

        self.major = socket.socket()
        port_major = 26
        self.major.bind(host, port_major)

        return

    def __call__(self, *args, **kwargs):
        self.major.listen(5)
        while True:
            self.mj_client, mj_address = self.major.accept()
            print 'Connection established with ', mj_address
            self.mj_client.send('Connected to Mapy interface')
        return

    def close_connection(self):
        self.mj_client.close()
        return
