#!/usr/bin/python3
import pickle
import time
import datetime
import os
import traceback
from os import path
from RepeatedTimer import RepeatedTimer
from collector import Collector, CollectorManager
import subprocess

import sys
import discord
from discord.ext import commands


print_native = print
def print(*args):
    try:
        print_native(*args)
    except OSError as e:
        pass


class ReportCollector(Collector):
    def get_suitable_pairs(self):
        pairs = []
        self.pair_volumes = {}
        with self.file('pairs', 'r') as f:
            for l in f:
                args = [x.strip() for x in l.split(',')]
                pair = (args[1], args[2])
                pairs.append(pair)
                self.pair_volumes[pair] = float(args[5])
        return pairs

    def load(f):
        state = pickle.load(f)
        collector_root = state['current_collector_root']
        pairs = state['pairs']
        collector = ReportCollector(collector_root, pairs=pairs)

        collector.pair_volumes = {}
        with collector.file('pairs', 'r') as f:
            for line in f:
                _, exchange, pair, _, _, volume = tuple(s.strip() for s in line.split(','))
                collector.pair_volumes[(exchange, pair)] = volume

        return collector

    def generate_report(self):
        return [self.pair_report(*p) for p in self.pairs if self.is_pair_good(*p)]

    def report(self):
        report = self.generate_report()
        return "\n".join(self.format_report_record(r) for r in report)

    @staticmethod
    def format_report_record(record):
        header = "{} : {}".format(record['exchange'], record['pair'])
        record['header'] = header
        record['spread'] = 1 - record['spread']
        record['avg_time'] = str(datetime.timedelta(seconds=int(record['avg_time']*100)))
        return """{header}
            volume={volume:.1f}\tord_count={order_count}\tspread={spread:.3f}
            time={avg_time}\tavg_volume={avg_volume:.6f}
            """.format(**record)

    def is_pair_good(self, exchange_name, pair):
        return self.spread(exchange_name, pair) >= 0.005

    def spread(self, exchange_name, pair):
        with self.file(("spread", exchange_name, pair), "r") as f:
            spread = 0.0
            count = 0
            for l in f:
                _, ask, bid = tuple(s.strip() for s in l.split(','))
                ask = float(ask)
                bid = float(bid)
                spread += (ask-bid)/ask
                count += 1
            spread /= count
            return spread

    def pair_report(self, exchange_name, pair):
        spread = self.spread(exchange_name, pair)
        volume = self.pair_volumes[(exchange_name, pair)]
        order_count = 0
        total_time = 0
        total_volume = 0
        with self.file(('trades', exchange_name, pair), 'r') as f:
            time_start = None
            time_end = None
            for line in f:
                data = tuple(s.strip() for s in line.split(','))
                _, timestamp, _, side, price, amount, br, amount_btc = data
                amount = float(amount)
                amount_btc = float(amount_btc)
                timestamp = int(timestamp)
                if br == 'break=True':
                    if time_start is not None:
                        total_time += (time_end - time_start) / 1000
                    time_start = timestamp

                time_end = timestamp
                order_count += 1
                total_volume += amount_btc
            total_time += (time_end - time_start) / 1000
            avg_time = total_time / order_count
            avg_volume = total_volume / order_count
            return {
                'exchange': exchange_name,
                'pair': pair, 
                'volume': float(volume),
                'avg_time': avg_time,
                'avg_volume': avg_volume,
                'order_count': order_count,
                'spread': self.spread(exchange_name, pair)
            }


class ReportManager(CollectorManager):
    def save_state(self):
        pass

    def collect(self):
        pass

    def new_collector(self):
        self.load_state()

    def report(self, back):
        date = datetime.datetime.now()
        delta = datetime.timedelta(days=back)
        date -= delta
        date = str(date.date())
        collector_root = path.join(self.root, date)
        collector = self.factory(collector_root)
        return collector.report()


if __name__ == '__main__':
    ROOT = "/home/hukumka/src/cryptostats/data/"
    bot = commands.Bot(command_prefix="&")
    report_manager = ReportManager(ROOT, factory=ReportCollector)

    @bot.event
    async def on_ready():
        print("ready")

    @bot.command()
    async def check():
        await bot.say("Bot status: online")
        ps = subprocess.Popen(('ps', '-A'), stdout=subprocess.PIPE)
        output = subprocess.check_output(('grep', 'collector.py'), stdin=ps.stdout)
        ps.wait()
        await bot.say("Collector status: " + output.decode('utf-8'))

    @bot.command()
    async def restart():
        os.system("./run.sh &")
        sys.exit(0)

    @bot.command()
    async def last_error():
        with open("data/collector_manager/last_error.txt", "r") as f:
            msg = f.read(f);
            await bot.say(msg)

    @bot.command()
    async def report(back:int=1):
        await bot.say("Собираю отчет")
        try:
            report = report_manager.report(back)
            if len(report) > 2000:
                r = [report[r:r+2000] for r in range(0, len(report), 2000)]
            else:
                r = [report]
            for i in r:
                await bot.say(i)
            await bot.say("Всёшеньки")
        except Exception as e:
            await bot.say(traceback.format_exc())
            raise e

    with open(sys.argv[1], 'r') as token_file:
        token = token_file.read()
        bot.run(token.strip())
