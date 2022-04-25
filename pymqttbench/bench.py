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
import signal
import threading
import random
import string
import uuid
import random
import time
from os import system
import sys

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

wait_pub_count = multiprocessing.Value('i',0)
def inc_wait_pub():
    global wait_pub_count
    with wait_pub_count.get_lock():
        wait_pub_count.value += 1

def dec_wait_pub():
    global wait_pub_count
    with wait_pub_count.get_lock():
        wait_pub_count.value -= 1

wait_connection = multiprocessing.Value('i',0)
def inc_wait_connection():
    global wait_connection
    with wait_connection.get_lock():
        wait_connection.value += 1

def dec_wait_connection():
    global wait_connection
    with wait_connection.get_lock():
        wait_connection.value -= 1


disconnected_pub_count = multiprocessing.Value('i',0)
def inc_disconnected_pub():
    global disconnected_pub_count
    with disconnected_pub_count.get_lock():
        disconnected_pub_count.value += 1

connected_sub_count = multiprocessing.Value('i',0)
def inc_connected_sub():
    global connected_sub_count
    with connected_sub_count.get_lock():
        connected_sub_count.value += 1

pub_issues_count = multiprocessing.Value('i',0)
def inc_pub_issues():
    global pub_issues_count
    with pub_issues_count.get_lock():
        pub_issues_count.value += 1

conn_issues_count = multiprocessing.Value('i',0)
def inc_conn_issues():
    global conn_issues_count
    with conn_issues_count.get_lock():
        conn_issues_count.value += 1

msgQueue = multiprocessing.Queue()
def log_relevant(msg):
    global msgQueue
    #print(msg)
    msgQueue.put(msg)

last_time = datetime.datetime.utcnow()
relevant_messages = []
def show_progress(subs, pubs, pub_count, force=False):
    global publishes_count, subscriber_recv_count, connected_pub_count, connected_sub_count, disconnected_pub_count, pub_issues_count
    global conn_issues_count, wait_pub_count, last_time, wait_connection
    global relevant_messages, msgQueue

    curr_time = datetime.datetime.utcnow()
    if (curr_time - last_time).total_seconds() >= 1 or force:
        #system("clear")
        last_time = curr_time
        print('------------------------')
        print('Connected - subs {} / {} '.format(connected_sub_count.value, subs))
        print('Connected - pubs {} / {} '.format(connected_pub_count.value, pubs))
        print('Waiting Connection - pubs {} / {} '.format(wait_connection.value, pubs))
        print('Connection issues - pubs {} / {} '.format(conn_issues_count.value, pubs))
        print('------------------------')
        print('Waiting for publish {} / {} '.format(wait_pub_count.value, pubs))
        print('Finished/Disconnected pubs {} / {} '.format(disconnected_pub_count.value, pubs))
        print('------------------------')
        print('Total Publish issues {} / {} '.format(pub_issues_count.value, pubs * pub_count))
        print('Total Published {} / {} '.format(publishes_count.value, pubs * pub_count))
        print('Total Received {} / {} '.format(subscriber_recv_count.value, pubs * pub_count * subs))
        print('------------------------')
        print('Relevant Messages')
        while True:
            msg = ''
            try:
                msg = msgQueue.get(False)
            except:
                break
            else:
                relevant_messages.insert(0,msg)
        for msg in relevant_messages :
            print('{}'.format(msg))
        print('------------------------')
        

class Sub(multiprocessing.Process):
    def __init__(self, hostname, port=1883, tls=None, auth=None, topic=None,
                 timeout=60, max_count=10, qos=0):
        super(Sub, self).__init__()
        self.hostname = hostname
        self.port = port
        self.tls = tls
        self.topic = topic or BASE_TOPIC
        self.auth = auth
        self.msg_count = 0
        self.start_time = None
        self.max_count = max_count
        self.end_time = None
        self.timeout = timeout
        self.qos = qos
        self.subscribed_evt = multiprocessing.Event()
        self.finished_evt = multiprocessing.Event()
    
    def start_wait_sub(self):
        self.start()
        timed_out = self.subscribed_evt.wait(self.timeout)
        if not timed_out :
            raise Exception('Failed to subscribe in time')

    def run(self):
        def on_connect(client, userdata, flags, rc):
            client.subscribe(BASE_TOPIC + '/#', qos=self.qos)
            self.subscribed_evt.set()
            inc_connected_sub()

        def on_message(client, userdata, msg):
            if self.start_time is None:
                self.start_time = datetime.datetime.utcnow()
            self.msg_count += 1
            inc_received_msg()
            self.end_time = datetime.datetime.utcnow()
            if self.msg_count >= self.max_count:
                self.finished_evt.set()
                

        self.client = mqtt.Client(client_id="{}-sub-{}".format(uuid.uuid1(), multiprocessing.current_process()))
        self.client.on_connect = on_connect
        self.client.on_message = on_message
        if self.tls:
            self.client.tls_set(**self.tls)
        if self.auth:
            self.client.username_pw_set(**self.auth)
        self.client.connect(self.hostname, port=self.port)
        self.client.loop_start()
        while True:
            end_evt = self.finished_evt.wait(1)
            if end_evt:
                delta = self.end_time - self.start_time
                SUB_QUEUE.put(delta.total_seconds())
                self.client.disconnect()
                self.client.loop_stop()
                break


class Pub(multiprocessing.Process):
    def __init__(self, hostname, port=1883, tls=None, auth=None, topic=None,
                 timeout=60, max_count=10, msg_size=1024, qos=0, disconnect_on_publish = True, wait_range = [0,0]):
        super(Pub, self).__init__()
        self.hostname = hostname
        self.port = port
        self.tls = tls
        self.topic = topic or BASE_TOPIC
        self.auth = auth
        self.start_time = None
        self.max_count = max_count
        self.end_time = None
        self.timeout = timeout
        self.msg = ''.join(
            random.choice(string.ascii_lowercase) for i in range(msg_size))
        self.qos = qos
        self.client = None
        self.finished_evt = multiprocessing.Event()
        self.disconnected_evt = multiprocessing.Event()
        self.connected_evt = multiprocessing.Event()
        self.disconnec_on_publish = disconnect_on_publish
        self.wait_range = wait_range
        self.wait_range.sort()

    def run(self):
        random.seed(uuid.uuid1())
        if not self.disconnec_on_publish :
            self.run_publish_not_disconnect()
        else:
            self.run_publish_disconnect()

    def run_publish_disconnect(self):
        self.start_time = datetime.datetime.utcnow()

        for i in range(self.max_count):
            self.finished_evt.wait(random.randint(*self.wait_range))
            inc_wait_pub()
            try:
                publish.single(self.topic, self.msg, hostname=self.hostname,
                           port=self.port, auth=self.auth, tls=self.tls,
                           qos=self.qos)
                inc_published_msg()
            except Exception as e:
                log_relevant("Expt: pub-{} - {} - Failed: {}/{} msg left".format(multiprocessing.current_process(), str(e), i+1, self.max_count))
                inc_pub_issues()
            dec_wait_pub()

        end_time = datetime.datetime.utcnow()
        delta = end_time - self.start_time
        PUB_QUEUE.put(delta.total_seconds())
        inc_disconnected_pub()

    
    def run_publish_not_disconnect(self):
        def on_connect(client, userdata, flags, rc):
            inc_connected_pub()
            self.connected_evt.set()

        def on_disconnect(client, userdata, rc):
            self.disconnected_evt.set()

        def on_publish(client, userdata, mid):
            inc_published_msg()

        self.client = mqtt.Client(client_id="{}-pub-{}".format(uuid.uuid1(), multiprocessing.current_process()))
        if self.tls:
            self.client.tls_set(**self.tls)
        if self.auth:
            self.client.username_pw_set(**self.auth)
        
        self.client.on_publish = on_publish
        self.client.on_disconnect = on_disconnect
        self.client.on_connect = on_connect
        self.start_time = datetime.datetime.utcnow()

        inc_wait_connection()
        while True:
            try:
                self.finished_evt.wait(random.randint(*self.wait_range))
                self.client.connect(self.hostname, port=self.port)
                self.client.loop_start()
                timed_out = self.connected_evt.wait(30)
                if not timed_out : #not connected in time, reconnect
                    log_relevant("{}-pub-{} - Not connected in time - retrying".format(uuid.uuid1(), multiprocessing.current_process()))
                    inc_conn_issues()
                    #inc_conn_issues()
                    self.client.disconnect()
                    timed_out = self.disconnected_evt.wait(1)
                    self.disconnected_evt.clear()
                    self.client.loop_stop()
                    #inc_disconnected_pub()
                    #return
                else: #connected!
                    self.disconnected_evt.clear()
                    dec_wait_connection()
                    break
            except Exception as e:
                log_relevant("Expt: {}-pub-{} - {} - retrying".format(uuid.uuid1(), multiprocessing.current_process(), str(e)))
                inc_conn_issues()
                self.client.loop_stop()
                #inc_disconnected_pub()
                #return

        
        for i in range(self.max_count):
            end_evt = self.finished_evt.wait(random.randint(*self.wait_range))
            if end_evt:
                break
            inc_wait_pub()
            res = self.client.publish(self.topic, self.msg, qos = self.qos)
            res.wait_for_publish(5)
            if not res.is_published():
                inc_pub_issues()
            dec_wait_pub()

        #self.client.disconnect()
        #timed_out = self.disconnected_evt.wait(10) #waits 10 seconds to disconnect
        #self.client.loop_stop()
        end_time = datetime.datetime.utcnow()
        delta = end_time - self.start_time
        PUB_QUEUE.put(delta.total_seconds())
        inc_disconnected_pub()


def main():
    global msgQueue
    
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
    parser.add_argument('--sub-count', type=int, dest='sub_count',
                        default=None,
                        help='The number of messages each subscriber client '
                             'will wait to recieve before completing. The '
                             'default count is 10.')
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

    parser.add_argument('--wait-range', nargs=2, default=[0,0], type=int, dest='wait_range',
                        help='Wait a random number of seconds, in range, before connecting or sending publishes')

    parser.add_argument('--disconnect-on-publish', default=True, action='store_true', dest='disconnect_on_publish',
                        help='If present the client will do connect, publish, disconnect (can take longer)')
    parser.add_argument('--no-disconnect-on-publish', action='store_false', dest='disconnect_on_publish',
                        help='If present the client will do not connect, publish, disconnect (can take longer)')

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

    if opts.sub_count is None : #expect to receive only the self published messages
        opts.sub_count = opts.pub_count * opts.pub_clients
   
    opts.wait_range.sort()
    log_relevant("Waiting {} - {} seconds between publishes".format(opts.wait_range[0], opts.wait_range[1]))

    for i in range(opts.sub_clients):
        
        sub = Sub(hostname=opts.hostname, 
                  port=opts.port, 
                  tls=tls, 
                  auth=auth, 
                  topic=topic, 
                  timeout=opts.sub_timeout,
                  max_count=opts.sub_count, 
                  qos=opts.qos)
        sub_threads.append(sub)
        sub.start_wait_sub()
        show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)


    for i in range(opts.pub_clients):
        pub = Pub(hostname=opts.hostname, 
                  port=opts.port, 
                  tls=tls, 
                  auth=auth, 
                  topic=topic, 
                  timeout=opts.pub_timeout,
                  max_count=opts.pub_count, 
                  qos=opts.qos,
                  disconnect_on_publish = opts.disconnect_on_publish, 
                  wait_range=opts.wait_range)
        pub_threads.append(pub)
        pub.start()
        show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)

    start_timer = datetime.datetime.utcnow()
    timeout = False
    last_terminated = 0
    while True : #waits for all publishers, or pub timeout
        terminated = 0
        for client in pub_threads:
            client.join(timeout=1)
            curr_time = datetime.datetime.utcnow()
            delta = curr_time - start_timer

            if delta.total_seconds() >= opts.pub_timeout:
                timeout = True
                break
            else:
                show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)
            
            if disconnected_pub_count.value == len(pub_threads):
                break
        if disconnected_pub_count.value == len(pub_threads) or timeout:
            break
    
    show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count, force=True) 

    if timeout : # kill all remaining publishers if timed out
        log_relevant('Pubs timedout terminate()ing them all')
        for client in pub_threads:
            client.terminate()
            client.join()
        log_relevant('Waiting for subs')
    else:
        log_relevant('Waiting for subs')

    start_timer = datetime.datetime.utcnow()
    timeout = False
    while True :
        terminated = 0
        for client in sub_threads:
            client.join(timeout=1)
            curr_time = datetime.datetime.utcnow()
            delta = curr_time - start_timer
            
            if delta.total_seconds() >= opts.sub_timeout:
                timeout = True
                break
            else:
                show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count)
            if client.exitcode != None :
                terminated = terminated + 1
        if terminated == len(sub_threads) or timeout:
            break

    show_progress(opts.sub_clients, opts.pub_clients, opts.pub_count, force=True) 

    if timeout : #terminate all subers
        log_relevant('Subs timedout finishing them')
        for client in sub_threads:
            client.finished_evt.set()
            client.join()

    # Queues can be different in sise since there could be connection issues

    sub_times = []
    for i in range(opts.sub_clients):
        try:
            sub_times.append(SUB_QUEUE.get(False))
        except multiprocessing.queues.Empty:
            continue
    if len(sub_times) < opts.sub_clients:
        failed_count = opts.sub_clients - len(sub_times)
    sub_times = numpy.array(sub_times)

    

    pub_times = []
    for i in range(disconnected_pub_count.value):
        try:
            pub_times.append(PUB_QUEUE.get(False))
        except multiprocessing.queues.Empty:
            continue

    failed_count = opts.pub_clients - disconnected_pub_count.value
    pub_times = numpy.array(pub_times)

    print("Pub times size {} | Sub times size {}".format(len(pub_times), len(sub_times)))
    sub_mean_duration = numpy.mean(sub_times)
    sub_avg_throughput = (subscriber_recv_count.value / opts.pub_clients) / float(sub_mean_duration)
    sub_total_thpt = float( subscriber_recv_count.value ) / float(sub_mean_duration)
    pub_mean_duration = numpy.mean(pub_times)
    pub_avg_throughput = float(publishes_count.value / opts.pub_count) / float(pub_mean_duration)
    pub_total_thpt = publishes_count.value / pub_mean_duration
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