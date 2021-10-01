#!/usr/bin/env python3
# author: thuhak.zhou@nio.com
from typing import Iterable
from collections import defaultdict
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor
from argparse import ArgumentParser
import operator
import json


parser = ArgumentParser()
parser.add_argument('subject', choices=['ib_read_lat', 'ib_write_lat', 'ib_read_bw', 'ib_write_bw'], default='ib_read_bw', help='test method')
parser.add_argument('-s', '--size', type=int, default=65536, help='message size')
parser.add_argument('--full', action='store_true', help='full connection test')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-q', '--queue', help='pbs queue')
group.add_argument('-l', '--list', help='path of server list file')
args = parser.parse_args()
method = args.subject
size = args.size


if method.endswith('bw'):
    unit = 'Gb/s'
    compare1 = max
    compare2 = lambda a, b: a < 0.9 * b
    parse_result = lambda result: float(result.split('\n')[-2].split()[-2])
else:
    unit = 'nsec'
    parse_result = lambda result: float(result.split('\n')[-2].split()[-5])
    compare1 = min
    compare2 = lambda a, b: a > 1.1 * b


queue = args.queue
if queue:
    nodes_raw = json.loads(subprocess.getoutput('/opt/pbs/bin/pbsnodes -Sa -F json'))['nodes']
    nodes = [x for x in nodes_raw if nodes_raw[x]['queue'] == queue and nodes_raw[x]['State'] == 'free']
else:
    with open(args.list) as f:
        nodes = [x.strip() for x in f.readlines()]

nodes.sort()


def avg(l):
    return sum(l)/len(l)


def generate_pairs(l: Iterable):
    function = operator.eq if args.full else operator.ge
    for i in range(len(l)):
        for j in range(len(l)):
            if function(i, j):
                continue
            yield l[i], l[j]


def create_table(l: Iterable):
    table = defaultdict(dict)
    for i in range(len(l)):
        for j in range(len(l)):
            table[i][j] = None
    return table


def test_ib(pair):
    a, b = pair
    p = '-q 2' if 'bw' in method else ''
    server_command = ['ssh', a, f'sudo {method} {p} -F --report_gbits -s {size}']
    server = subprocess.Popen(server_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    data = subprocess.getoutput(f"ssh {b} 'sudo {method} {p} -F {a} --report_gbits -s {size}'")
    try:
        server.wait(5)
    except:
        server.kill()
    try:
        result = parse_result(data)
    except:
        result = 0
    return [a, b], result


if __name__ == '__main__':
    test_cases = list(generate_pairs(nodes))
    nodes = defaultdict(list)
    results = []
    h = queue if queue else args.list
    print(f'test {method} for {h}')
    print('----')
    while test_cases:
        testservers = set()
        job = []
        for i, pair in enumerate(test_cases):
            if all([x not in testservers for x in pair]):
                pair = test_cases.pop(i)
                job.append(pair)
                testservers.update(pair)
        with ProcessPoolExecutor(max_workers=20) as pool:
            for pair, result in pool.map(test_ib, job):
                print(f'{pair[0]}-{pair[1]}: {result}({unit})')
                nodes[pair[0]].append(result)
                nodes[pair[1]].append(result)
                results.append(result)

    target = compare1(results)
    print('----------')
    for node, value_list in nodes.items():
        if compare2(compare1(value_list), target):
            print(f'{node} speed is too low')
        elif compare2(avg(value_list), target):
            print(f'{node} average speed is too low')
