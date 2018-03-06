#!/usr/bin/python2.7
from argparse import ArgumentParser
from collections import deque
from csv import reader
from itertools import count, cycle, islice
from operator import itemgetter
from sys import exit
from urllib2 import URLError, urlopen

class Server:
    def __init__(self):
        self.time_remaining = 0

    def tick(self):
        self.time_remaining -= 1

    def is_busy(self):
        return self.time_remaining > 0

    def start_next(self, new_request):
        self.time_remaining = new_request.process_time

class Request:
    def __init__(self, request_time, process_time):
        self.request_time = request_time
        self.process_time = process_time

    def wait_time(self, current_time):
        return current_time - self.request_time
        
def simulateOneServer(requests):
    # Create the resources, using deque because it is in the standard library.
    server = Server()
    queue = deque()  
    waiting_times = []
    
    # Create all requests immediately as they are predetermined.
    for request_time, resource, process_time in requests:
        queue.append(Request(request_time, process_time))
    
    # Return early if there were no requests.
    if not len(queue):
        return None
    
    # Tick indefinitely.
    for current_time in count():
        # The request time must be checked to avoid starting a request early.
        if not server.is_busy() and queue[0].request_time <= current_time:
            next_request = queue.popleft()
            waiting_times.append(next_request.wait_time(current_time))
            server.start_next(next_request)
            if not len(queue):
                return float(sum(waiting_times)) / len(waiting_times)
            
        server.tick()
    
def simulateManyServers(requests, num_servers):
    # Create the resources, using deque because it is in the standard library.
    servers = cycle([Server() for _ in range(num_servers)])
    server = next(servers)
    queue = deque()
    waiting_times = []
    
    # Create all requests immediately as they are predetermined.
    for request_time, resource, process_time in requests:
        queue.append(Request(request_time, process_time))
    
    # Return early if there were no requests.
    if not len(queue):
        return None
    
    # Tick indefinitely.
    for current_time in count():
        # The request time must be checked to avoid starting a request early.
        while not server.is_busy() and queue[0].request_time <= current_time:
            next_request = queue.popleft()
            waiting_times.append(next_request.wait_time(current_time))
            server.start_next(next_request)
            server = next(servers)
            if not len(queue):
                return float(sum(waiting_times)) / len(waiting_times)
        
        # An exact cycle preserves the iterator state and current server.
        for server in islice(servers, num_servers):
            server.tick()

# Creates a CSV reader for the data at the given URL.
def download_data(url):
    for request_time, resource, process_time in reader(urlopen(url)):
        yield int(request_time), resource, int(process_time)
    
if __name__ == "__main__":
    # Parse the --url argument.
    parser = ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--servers", type=int, required=False)
    args = parser.parse_args()

    # Read in the data from the URL. The assignment asked for this to be done
    # in both the main function and simulateOneSever, which isn't necessary.
    try:
        csv_data = list(download_data(args.url))[:100]
    except URLError:
        print "Could not fetch data from the given URL:", url
        exit()
    except ValueError:
        print "Invalid URL given:", url
        exit()
    
    if args.servers:
        wait_time = simulateManyServers(csv_data, args.servers)
    else:
        wait_time = simulateOneServer(csv_data)
        
    print "Average Wait is %.2f secs." % wait_time
    