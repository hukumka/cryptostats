#!/usr/bin/python3

import signal
import argparse
import os
import pickle
import threading
import time
import subprocess

from os import path
from datetime import datetime

import exchange
from RepeatedTimer import RepeatedTimer


ROOT = "/home/hukumka/src/cryptostats/data/"


class Collector:
    EXCHANGES = ('exmo',)
    BTC_VOLUME_LIMIT = 20

    def __init__(self, root, pairs=None):
        self.csv_list = {
            'log': 'collector_log.csv',
            'all_pairs': 'all_pairs.csv',
            'pairs': 'pairs.csv',
        }
        self.path = root
        if not path.isdir(self.path):
            os.mkdir(self.path)
        if pairs is None:
            self.pairs = self.get_suitable_pairs()
        else:
            self.pairs = pairs
        print("Selected pairs:")
        print(self.pairs)
        print('------------------')

        self.log('log', 'Collector.__init__("{}")'.format(root))
        
    def log(self, *args):
        file_descriptor = args[0]
        data = [str(datetime.now())] + list(args[1:])
        with self.file(file_descriptor) as f:
            f.write(', '.join(str(d) for d in data) + '\n')

    def file(self, id, param="a"):
        return open(self.file_path(id), param)

    def file_path(self, id):
        if isinstance(id, str):
            if id in self.csv_list:
                return path.join(self.path, self.csv_list[id])
            else:
                raise Exception("Unknown log id: {}".format(id))
        else:
            return path.join(self.path, "generic_" + "_".join(i.replace("/", "-") for i in id) + ".csv")

    def last_line(self, id):
        return subprocess.check_output(['tail', '-1', self.file_path(id)])

    def get_suitable_pairs(self):
        result = []
        for e in self.EXCHANGES:
            api = exchange.api_by_name(e)
            pairs = exchange.PairGraph(api)
            for pair in pairs.markets:
                if self.is_pair_suitable(e, pairs, pair):
                    result.append((e, pair['symbol']))
        return result


    def is_pair_suitable(self, exchange, pairs_graph, pair):
        print_if_verbose("Checking pair {}:\"{}\"".format(exchange, pair['symbol']))
        ticker = pairs_graph.exchange.fetch_ticker(pair['symbol'])
        bid_volume, quote_volume = ticker['bidVolume'], ticker['quoteVolume']
        btc_volume = pairs_graph.convert_currency(pair['quote'], 'BTC', quote_volume)
        self.log('all_pairs', exchange, pair['symbol'], bid_volume, quote_volume, btc_volume)
        print_if_verbose(exchange, pair['symbol'], bid_volume, quote_volume, btc_volume, btc_volume > self.BTC_VOLUME_LIMIT)
        if btc_volume > self.BTC_VOLUME_LIMIT:
            self.log('pairs', exchange, pair['symbol'], bid_volume, quote_volume, btc_volume)
            return True
        else:
            return False


    def collect(self):
        print("Collector.collect({})".format(self.path))
        self.log('log', 'Collector.collect')
        for exchange_name, pair in self.pairs:
            self.collect_trades(exchange_name, pair)

    def collect_trades(self, exchange_name, pair):
        print("Collector.collect_trades({}, {})".format(exchange_name, pair))
        file_id = (exchange_name, pair)
        try:
            last_trade_data = self.last_line(file_id).decode('utf-8')
            last_trade_data = [x.strip() for x in last_trade_data.split(',')]
            timestamp = int(x[1])
            trade_id = int(x[2])
            print_if_verbose("Log found: keep adding records with timestamp>{}".format(timestamp))
            new_only = True
        except subprocess.CalledProcessError:
            timestamp = 0
            trade_id = None
            print_if_verbose("Log not found: create new and add all records")
            new_only = True

        trades = exchange.api_by_name(exchange_name).fetch_trades(pair)
        trades.sort(key=lambda x: (x['timestamp'], x['id']))
        for t in trades:
            if t['timestamp'] >= timestamp and t['id'] != trade_id:
                timestamp = t['timestamp']
                trade_id = t['id']
                side = t['side']
                price = t['price']
                amount = t['amount']
                self.log(file_id, timestamp, trade_id, side, price, amount, "break="+str(new_only))
                new_only = True
                print_if_verbose("Add record timestamp={} id={} volume={} price={}".format(timestamp, trade_id, amount, price))
            else:
                new_only = False
                print_if_verbose("Skip record with timestamp {}<{}".format(t['timestamp'], timestamp))
        self.log('log', 'Collector.collect_trades', exchange_name, pair)

    def save_state(self, f):
        pickle.dump({
            'current_collector_root': self.path,
            'pairs': self.pairs
        }, f)

    def load(f):
        state = pickle.load(f)
        collector_root = state['current_collector_root']
        pairs = state['pairs']
        collector = Collector(collector_root, pairs=pairs)
        collector.log('log', 'Collector.load({}, {})'.format(collector_root, pairs))
        return collector



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
            self.collector.save_state(f)

    def load_state(self):
        try:
            with open(path.join(self.root, "collector_manager", self.state_file), 'rb') as f:
                self.collector = self.factory.load(f)
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


class Args:
    def __init__(self):
        self.verbose = False

cmd_args = Args()
def print_if_verbose(*args, **kwargs):
    if cmd_args.verbose:
        print(*args, *kwargs)



if __name__ == '__main__':
    # setup sigint and sigterm handling
    running = True
    def stop_running(signum, frame):
        global running
        print("Terminated", signum, frame)
        running = False
    signal.signal(signal.SIGINT, stop_running)
    signal.signal(signal.SIGTERM, stop_running)

    parser = argparse.ArgumentParser(description='Run collector process')
    parser.add_argument("-d", "--drop", action="store_true", help="ignore previous state")
    parser.add_argument("-v", "--verbose", action="store_true", help="Extra level of verbosity")
    cmd_args = parser.parse_args()
    manager = CollectorManager(ROOT, forget_state=cmd_args.drop)

    print(cmd_args)

    INTERVAL = 60


    def collect():
        if manager.is_old():
            manager.take_collected()
            print_if_verbose("new manager created")

        manager.collect()
        print_if_verbose("collected")

    try:
        timer = RepeatedTimer(INTERVAL, collect)
        collect()  # timer will call only after interval passes
        while running:
            pass
    finally:
        print("Cleaning up")
        timer.stop()
