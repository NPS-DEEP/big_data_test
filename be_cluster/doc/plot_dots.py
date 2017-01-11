#!/usr/bin/env python3
#
# graph timing from timing file

import sys
import pylab
import numpy
import matplotlib.pyplot as plot
from argparse import ArgumentParser
from collections import defaultdict

time_xlabel="Time in seconds"
bytes_ylabel="GB processed"

def parse_pair(line):
    h = line[11:13]
    m = line[14:16]
    s = line[17:19]
    time = (int(h) * 60 + int(m)) * 60 + int(s)
    count = int(line[24:])
    return time, count

def read_timing(timing_file):
    time_list = list()
    total_bytes_list = list()

    with open(timing_file, 'r') as infile:
        first = True
        offset = 0
        total = 0
        for line in infile:
           time, count = parse_pair(line)
           if first:
               offset = time
               first = False
           time_list.append(time - offset)
           total += count
           total_bytes_list.append(total/1000000000.0)
    return (time_list, total_bytes_list)

def plot_timing(time_list, total_bytes_list, outfile):
    plot.plot(time_list, total_bytes_list, '-bo')
    plot.xlabel(time_xlabel)
    plot.ylabel(bytes_ylabel)
    plot.title("Bytes processed over time")
    plot.savefig(outfile)

if __name__=="__main__":
    parser = ArgumentParser(prog='plot_dots.py',
                  description='Plot timestamp progress')
    parser.add_argument('timing_file', help= 'timing file')
    args = parser.parse_args() 
    timing_file = args.timing_file
    if timing_file[:7] != 'logfile':
        raise ValueError("Invalid prefix '%s'" % timing_file[:7])

    time_list, total_bytes_list = read_timing(timing_file)
    for time in time_list:
        print("time: %d\n" % time)
    for total_bytes in total_bytes_list:
        print("total bytes: %d\n" % total_bytes)
    outfile = "out_%s.png" % timing_file
    plot_timing(time_list, total_bytes_list, outfile)
