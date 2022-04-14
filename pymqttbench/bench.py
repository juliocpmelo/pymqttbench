# Copyright 2017 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import argparse
import datetime
import multiprocessing
import threading
import random
import string
import time
from os import system

import numpy
import paho.mqtt.client as mqtt
from paho.mqtt import publish

BASE_TOPIC = 'pybench'

SUB_QUEUE = multiprocessing.Queue()
PUB_QUEUE = multiprocessing.Queue()

publishes_count = multiprocessing.Value('i',0)
def inc_published_msg():
    global publishes_count
    with publishes_count.get_lock():
        publishes_count.value += 1


subscriber_recv_count = multiprocessing.Value('i',0)
def inc_received_msg():
    global subscriber_recv_count
    with subscriber_recv_count.get_lock():
        subscriber_recv_count.value += 1

connected_pub_count = multiprocessing.Value('i',0)
def inc_connected_pub():
    global connected_pub_count
    with connected_pub_count.get_lock():
        connected_pub_count.value += 1

connected_sub_count = multiprocessing.Value('i',0)
def inc_connected_sub():
    global connected_sub_count
    with connected_sub_count.get_lock():
        connected_sub_count.value += 1

def show_progress(subs, pubs, pub_count):
    global publishes_count, subscriber_recv_count, connected_pub_count, connected_sub_count
    #system("clear")
    print('------------------------')
    print('Connected - subs {} / {} '.format(connected_sub_count.value, subs))
    print('Connected - pubs {} / {} '.format(connected_pub_count.value, pubs))
    print('Published {} / {} '.format(publishes_count.value, pubs * pub_count))
    print('Received {} / {} '.format(subscriber_recv_count.value, pubs * pub_count * subs))
    print('------------------------')

class Sub(multiprocessing.Process):
    def __init__(self, hostname, port=1883, tls=None, auth=None, topic=None,
                 timeout=60, max_count=10, qos=0, n_connections = 1):
        super(Sub, self).__init__()
        self.hostname = hostname
        self.port = port
        self.tls = tls
        self.topic = topic or BASE_TOPIC
        self.auth = auth
        self.msg_count = [0]*n_connections
        self.msg_count_mutex = [threading.Lock()]*n_connections
        self.start_time = [None]*n_connections
        self.max_count = max_count
        self.end_times = [None]*n_connections
        self.timeout = timeout
        self.qos = qos
        self.subscribed_evt = multiprocessing.Event()
        self.n_connected_mutex = multiprocessing.Lock()
        self.n_connections = n_connections
        self.n_connected = 0
    
    def start_wait_sub(self):
        self.start()
        timed_out = self.subscribed_evt.wait(self.timeout)
        if not timed_out :
            raise Exception('Failed to subscribe in time')


    def run(self):
        def on_connect(client, userdata, flags, rc):
            inc_connected_sub()
            client.subscribe(BASE_TOPIC + '/#', qos=self.qos)
            client_idx = self.clients.index(client)
            self.n_connected_mutex.acquire()
            self.n_connected = self.n_connected + 1
            if self.n_connected == self.n_connections :
                self.subscribed_evt.set()
            self.n_connected_mutex.release()

        def on_connect_fail(client, userdata):
            client_idx = self.clients.index(client)
            print('connection fail for {} in thread {}'.format(client,multiprocessing.current_process()))

        def on_message(client, userdata, msg):
            inc_received_msg()

            client_idx = self.clients.index(client)

            self.msg_count_mutex[client_idx].acquire()
            if self.start_time[client_idx] is None:
                self.start_time[client_idx] = datetime.datetime.utcnow()
            #print("Worker {} received message #{}".format(multiprocessing.current_process(), self.msg_count))

            self.msg_count[client_idx] += 1
            if self.msg_count[client_idx] >= self.max_count:
                print("Worker {} finished msg count/max {}/{}".format(multiprocessing.current_process(), self.msg_count[client_idx], self.max_count))
                if self.end_times[client_idx] is None:
                    self.end_times[client_idx] = datetime.datetime.utcnow()
                    delta = self.end_times[client_idx] - self.start_time[client_idx]
                    SUB_QUEUE.put(delta.total_seconds())
            self.msg_count_mutex[client_idx].release()
        
        
        self.clients = [None]*self.n_connections
        
        print('Creating {} connections for sub-thread {} - max count {} '.format(self.n_connections, multiprocessing.current_process(), self.max_count))
        for i in range(self.n_connections):
            self.clients[i] = mqtt.Client(client_id="sub{}-{}".format(i, multiprocessing.current_process()))
            self.clients[i].on_connect = on_connect
            self.clients[i].on_message = on_message
            self.clients[i].on_connect_fail = on_connect_fail
            
            if self.tls:
                self.clients[i].tls_set(**self.tls)
            if self.auth:
                self.clients[i].username_pw_set(**self.auth)
            self.clients[i].connect(self.hostname, port=self.port)
            self.clients[i].loop_start()

        start_time = datetime.datetime.utcnow()
        while True: #somehow subscribers wont receive all msgs
            time.sleep(1)
            finished = 0
            for i in range(self.n_connections):
                if self.end_times[i] != None:
                    if self.clients[i].loop_stop():
                        finished = finished + 1
            if finished == self.n_connections:
                break
            current_time = datetime.datetime.utcnow()
            curr_delta = current_time - start_time
            if curr_delta.total_seconds() > self.timeout:
                raise Exception('We hit the sub timeout!')


class Pub(multiprocessing.Process):
    def __init__(self, hostname, port=1883, tls=None, auth=None, topic=None,
                 timeout=60, max_count=10, msg_size=1024, qos=0, n_connections = 1):
        super(Pub, self).__init__()
        self.hostname = hostname
        self.port = port
        self.tls = tls
        self.topic = topic or BASE_TOPIC
        self.auth = auth
        self.start_times = [None]*n_connections
        self.end_times = [None]*n_connections
        self.max_count = max_count
        self.n_published = [0]*n_connections
        self.n_published_mutexes = [threading.Lock()]*n_connections
        self.publish_elapsed_times = [None]*n_connections
        self.n_connections = n_connections
        self.timeout = timeout
        self.msg = ''.join(
            random.choice(string.ascii_lowercase) for i in range(msg_size))
        self.qos = qos
        self.mqtt_clients = [None]*n_connections#mqtt.Client()

    def run(self):

        def on_connect(client, userdata, flags, rc):
            inc_connected_pub()
            client.publish(self.topic, self.msg, qos = self.qos)

        def on_publish(client, userdata, mid):
            client_idx = self.mqtt_clients.index(client)
            inc_published_msg()

            #print("Worker {} trying to publish msg #{}".format(multiprocessing.current_process(), self.n_published[client_idx]))
            self.n_published_mutexes[client_idx].acquire()
            self.n_published[client_idx] += 1
            #print("Worker {} published msg #{}".format(multiprocessing.current_process(), self.n_published[client_idx]))
            if self.n_published[client_idx] >= self.max_count :
                #print("Worker {} finished msg count/max {}/{}".format(multiprocessing.current_process(), self.n_published[client_idx], self.end_times[client_idx]))
                if self.end_times[client_idx] is None :
                    self.end_times[client_idx] = datetime.datetime.utcnow() 
                    publish_elapsed_time = self.end_times[client_idx] - self.start_times[client_idx]
                    PUB_QUEUE.put(publish_elapsed_time.total_seconds())
            else:
                #print("Worker {} trying to publish msg #{}".format(multiprocessing.current_process(), self.n_published[client_idx]+1))
                client.publish(self.topic, self.msg, qos = self.qos)
            self.n_published_mutexes[client_idx].release()
        
        print('Creating {} connections for pub-thread {}'.format(self.n_connections, multiprocessing.current_process()))
        for i in range(self.n_connections):
            self.mqtt_clients[i] = mqtt.Client(client_id="pub{}-{}".format(i, multiprocessing.current_process()))
            if self.tls:
                self.mqtt_clients[i].tls_set(**self.tls)
            if self.auth:
                self.mqtt_clients[i].username_pw_set(**self.auth)
            
            self.mqtt_clients[i].on_publish = on_publish
            self.mqtt_clients[i].on_connect = on_connect
            self.start_times[i] = datetime.datetime.utcnow()
            self.mqtt_clients[i].connect(self.hostname, port=self.port)
            self.mqtt_clients[i].loop_start()
            
        

        start_time = datetime.datetime.utcnow()
        while True: 
            time.sleep(1)
            finished = 0
            for i in range(self.n_connections):
                if self.end_times[i] != None:
                    if self.mqtt_clients[i].loop_stop():
                        finished = finished + 1
            if finished == self.n_connections:
                break
                #print("Worker publisher {} finished ".format(multiprocessing.current_process()))
                #this connects and publish in a single method, not sure if it keeps connection active
                #publish.single(topic=self.topic, payload=self.msg, hostname=self.hostname, port=self.port,
                #                client_id = "pub{}-{}".format(j, multiprocessing.current_process()), 
                #                tls=self.tls, auth=self.auth,  qos = self.qos)
                
                
                #publish_time_e = datetime.datetime.utcnow()
                #if self.publish_elapsed_times[j] == None :
                #    self.publish_elapsed_times[j] = publish_time_e - publish_time_b
                #else:
                #    self.publish_elapsed_times[j] += publish_time_e - publish_time_b
                #print("Worker {} published message #{}".format(multiprocessing.current_process(), i))
            #timeout test
            runnig_time = datetime.datetime.utcnow() - start_time
            if runnig_time.total_seconds() > self.timeout:
                for k in range(self.n_connections):
                    self.mqtt_clients[k].disconnect()
                raise Exception('We hit the pub timeout!')
        #for j in range(self.n_connections):
        #    PUB_QUEUE.put(self.publish_elapsed_times[j].total_seconds())
        for j in range(self.n_connections):
            self.mqtt_clients[j].disconnect()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pub-clients', type=int, dest='pub_clients',
                        default=10,
                        help='The number of publisher client workers to use. '
                             'By default 10 are used.')
    parser.add_argument('--sub-clients', type=int, dest='sub_clients',
                        default=10,
                        help='The number of subscriber client workers to use. '
                             'By default 10 are used')
    parser.add_argument('--pub-count', type=int, dest='pub_count',
                        default=10,
                        help='The number of messages each publisher client '
                             'will publish for completing. The default count '
                             'is 10')
    parser.add_argument('--sub-count', type=int, default=None, dest='sub_count',
                        help='The number of messages each subscriber client '
                             'will wait to recieve before completing. The '
                             'default count is pub_count.')
    parser.add_argument('--msg-size', type=int, dest='msg_size', default=1024,
                        help='The payload size to use in bytes')
    parser.add_argument('--sub-timeout', type=int, dest='sub_timeout',
                        default=60,
                        help='The amount of time, in seconds, a subscriber '
                             'client will wait for messages. By default this '
                             'is 60.')
    parser.add_argument('--pub-timeout', type=int, dest='pub_timeout',
                        default=60,
                        help="The amount of time, in seconds, a publisher "
                             "client will wait to successfully publish it's "
                             "messages. By default this is 60")
    parser.add_argument('--hostname', required=True,
                        help='The hostname (or ip address) of the broker to '
                             'connect to')
    parser.add_argument('--port', default=1883, type=int,
                        help='The port to use for connecting to the broker. '
                             'The default port is 1883.')
    parser.add_argument('--topic',
                        help='The MQTT topic to use for the benchmark. The '
                             'default topic is pybench')
    parser.add_argument('--cacert',
                        help='The certificate authority certificate file that '
                             'are treated as trusted by the clients')
    parser.add_argument('--username',
                        help='An optional username to use for auth on the '
                             'broker')
    parser.add_argument('--password',
                        help='An optional password to use for auth on the '
                             'broker. This requires a username is also set')
    parser.add_argument('--brief', action='store_true', default=False,
                        help='Print results in a colon separated list instead'
                             ' of a human readable format. See the README for '
                             'the order of results in this format')
    parser.add_argument('--qos', default=0, type=int, choices=[0, 1, 2],
                        help='The qos level to use for the benchmark')
    parser.add_argument('--threads', default=8, type=int,
                        help='The number of threads to create, defaults to 8')

    opts = parser.parse_args()

    sub_threads = []
    pub_threads = []

    topic = getattr(opts, 'topic') or BASE_TOPIC
    tls = None
    if getattr(opts, 'cacert'):
        tls = {'ca_certs': opts.cacert}

    auth = None
    if opts.username:
        auth = {'username': opts.username,
                'password': getattr(opts, 'password')}

    if opts.sub_count is not None:
        if opts.pub_count * opts.pub_clients < opts.sub_count:
            print('The configured number of publisher clients and published '
                'message count is too small for the configured subscriber count.'
                ' Increase the value of --pub-count and/or --pub-clients, or '
                'decrease the value of --sub-count.')
            exit(1)
    else :
        opts.sub_count = opts.pub_count * opts.pub_clients

    # the total amount of threads is at most opts.threads * 2 + 2, will keep this way for simplicity
    # maybe change the name of parameter to make it more clear
    if opts.sub_clients // opts.threads > 0 :
        for i in range(opts.threads):
            sub = Sub(opts.hostname, opts.port, tls, auth, topic, opts.sub_timeout,
                    opts.sub_count, opts.qos, opts.sub_clients // opts.threads)
            sub_threads.append(sub)
            sub.start_wait_sub()
    if  opts.sub_clients % opts.threads != 0: #this will possibly create opts.threads + 1 threads instead of the exact number, for simplicity
        sub = Sub(opts.hostname, opts.port, tls, auth, topic, opts.sub_timeout,
                  opts.sub_count, opts.qos, opts.sub_clients % opts.threads)
        sub_threads.append(sub)
        sub.start_wait_sub()
        
    if opts.pub_clients // opts.threads > 0 :
        for i in range(opts.threads):
            pub = Pub(opts.hostname, opts.port, tls, auth, topic, opts.pub_timeout,
                    opts.pub_count, opts.msg_size, opts.qos, opts.pub_clients // opts.threads)
            pub_threads.append(pub)
            pub.start()
    if  opts.pub_clients % opts.threads != 0: #this will possibly create opts.threads + 1 threads instead of the exact number, for simplicity
        pub = Pub(opts.hostname, opts.port, tls, auth, topic, opts.pub_timeout,
                  opts.pub_count, opts.msg_size, opts.qos, opts.pub_clients % opts.threads)
        pub_threads.append(pub)
        pub.start()
#
#    for i in range(opts.pub_clients):
#        pub = Pub(opts.hostname, opts.port, tls, auth, topic, opts.pub_timeout,
#                  opts.pub_count, opts.qos)
#        pub_threads.append(pub)
#        pub.start()
   
    start_timer = datetime.datetime.utcnow()
    while True : #loops until all terminate
        terminated = 0
        for client in pub_threads:
            client.join(1)
            if client.exitcode != None :
                terminated = terminated + 1
            show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)
            curr_time = datetime.datetime.utcnow()
            delta = start_timer - curr_time
            if delta.total_seconds() >= opts.sub_timeout:
                raise Exception('Timed out waiting for threads to return')
        if terminated == len(pub_threads) :
            break

    start_timer = datetime.datetime.utcnow()
    while True :
        terminated = 0
        for client in sub_threads:
            client.join(1)
            if client.exitcode != None :
                    terminated = terminated + 1
            show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)
            curr_time = datetime.datetime.utcnow()
            delta = start_timer - curr_time
            if delta.total_seconds() >= opts.sub_timeout:
                raise Exception('Timed out waiting for threads to return')
        if terminated == len(sub_threads) :
            break
    
    show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)

    # Let's do some maths
    if SUB_QUEUE.qsize() < opts.sub_clients:
        print('Something went horribly wrong, there are less results than '
              'sub threads {} < {}'.format(SUB_QUEUE.qsize(), opts.sub_clients))
        exit(1)
    if PUB_QUEUE.qsize() < opts.pub_clients:
        print('Something went horribly wrong, there are less results than '
              'pub threads {} < {}'.format(PUB_QUEUE.qsize(), opts.pub_clients))
        exit(1)

    sub_times = []
    for i in range(opts.sub_clients):
        try:
            sub_times.append(SUB_QUEUE.get(opts.sub_timeout))
        except multiprocessing.queues.Empty:
            continue
    if len(sub_times) < opts.sub_clients:
        failed_count = opts.sub_clients - len(sub_times)
    sub_times = numpy.array(sub_times)

    pub_times = []
    for i in range(opts.pub_clients):
        try:
            pub_times.append(PUB_QUEUE.get(opts.pub_timeout))
        except multiprocessing.queues.Empty:
            continue
    if len(pub_times) < opts.pub_clients:
        failed_count = opts.pub_clients - len(pub_times)
    pub_times = numpy.array(pub_times)

    if len(sub_times) < opts.sub_clients:
        failed_count = opts.sub_clients - len(sub_times)
        print("%s subscription workers failed" % failed_count)
    if len(pub_times) < opts.pub_clients:
        failed_count = opts.pub_clients - len(pub_times)
        print("%s publishing workers failed" % failed_count)

    sub_mean_duration = numpy.mean(sub_times)
    sub_avg_throughput = float(opts.sub_count) / float(sub_mean_duration)
    sub_total_thpt = float(
        opts.sub_count * opts.sub_clients) / float(sub_mean_duration)
    pub_mean_duration = numpy.mean(pub_times)
    pub_avg_throughput = float(opts.pub_count) / float(pub_mean_duration)
    pub_total_thpt = float(
        opts.pub_count * opts.pub_clients) / float(pub_mean_duration)
    if opts.brief:
        output = '%s:%s:%s:%s:%s:%s:%s:%s:%s:%s'
    else:
        output = """\
[ran with %s subscribers and %s publishers]
================================================================================
Subscription Results
================================================================================
Avg. subscriber duration: %s
Subscriber duration std dev: %s
Avg. Client Throughput: %s
Total Throughput (msg_count * clients) / (avg. sub time): %s
================================================================================
Publisher Results
================================================================================
Avg. publisher duration: %s
Publisher duration std dev: %s
Avg. Client Throughput: %s
Total Throughput (msg_count * clients) / (avg. sub time): %s
"""
    print(output % (
        opts.sub_clients,
        opts.pub_clients,
        sub_mean_duration,
        numpy.std(sub_times),
        sub_avg_throughput,
        sub_total_thpt,
        pub_mean_duration,
        numpy.std(pub_times),
        pub_avg_throughput,
        pub_total_thpt,
        ))


if __name__ == '__main__':
    main()
