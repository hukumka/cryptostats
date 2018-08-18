#!/usr/bin/python3

import signal
import argparse
import os
import pickle
import threading
import time

from os import path
from datetime import datetime

from RepeatedTimer import RepeatedTimer


ROOT = "/home/hukumka/src/cryptostats/data/"


class Collector:
    def __init__(self, root):
        self.csv_list = {
            'log': 'collector_log.csv'
        }
        self.path = root
        if not path.isdir(self.path):
            os.mkdir(self.path)

        self.log('log', 'Collector.__init__("{}")'.format(root))
        
    def log(self, *args):
        file_descriptor = args[0]
        data = [str(datetime.now())] + list(args[1:])
        with self.file(file_descriptor) as f:
            f.write(', '.join(data) + '\n')

    def file(self, id, param="a"):
        return open(path.join(self.path, self.csv_list[id]), param)

    def collect(self):
        self.log('log', 'Collector.collect')
        

class CollectorManager:
    def __init__(self, root, 
            factory=Collector, 
            forget_state=False,
            state_file='state.pickle'
    ):
        self.root = root
        self.factory = factory
        self.state_file = state_file
        if forget_state or not self.load_state() :
            self.__new_collector()

    def save_state(self):
        with open(path.join(self.root, "collector_manager", self.state_file), 'wb') as f:
            pickle.dump({
                'current_collector_root': self.collector.path,
            }, f)

    def load_state(self):
        try:
            with open(path.join(self.root, "collector_manager", self.state_file), 'rb') as f:
                state = pickle.load(f)

            collector_root = state['current_collector_root']
            self.collector = self.factory(collector_root)
            return True
        except (FileNotFoundError, KeyError):
            return False

    def collect(self):
        self.collector.collect()

    def is_old(self):
        date = str(datetime.now().date())
        collector_root = path.join(self.root, date)
        return collector_root != self.collector.path

    def take_collected(self):
        collector_root = self.collector.path
        self.__new_collector()
        return collector_root

    def __new_collector(self):
        date = str(datetime.now().date())
        collector_root = path.join(self.root, date)
        self.collector = self.factory(collector_root)
        self.save_state()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run collector process')
    parser.add_argument("-d", "--drop", action="store_true", help="ignore previous state")
    parser.add_argument("-v", "--verbose", action="store_true", help="Extra level of verbosity")
    cmd_args = parser.parse_args()
    manager = CollectorManager(ROOT, forget_state=cmd_args.drop)

    print(cmd_args)

    def print_if_verbose(*args, **kwargs):
        if cmd_args.verbose:
            args = [datetime.now()] + list(args)
            print(*args, *kwargs)

    INTERVAL = 60

    def collect():
        if manager.is_old():
            manager.take_collected()
            print_if_verbose("new manager created")

        manager.collect()
        print_if_verbose("collected")

    try:
        # setup sigint and sigterm handling
        running = True
        def stop_running(signum, frame):
            global running
            print("Terminated", signum, frame)
            running = False

        signal.signal(signal.SIGINT, stop_running)
        signal.signal(signal.SIGTERM, stop_running)

        timer = RepeatedTimer(INTERVAL, collect)
        collect()  # timer will call only after interval passes
        while running:
            pass
    finally:
        print("Cleaning up")
        timer.stop()
