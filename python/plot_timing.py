#!/usr/bin/env python3
#
# usage: plot_timing.py timing_file
#
# graph timing from timing_file to temp_deltas.pdf and temp_totals.pdf

import sys
import pylab
import numpy
import matplotlib.pyplot as plot
from argparse import ArgumentParser
from collections import defaultdict

plotsize=(6,3.5)
out_pdf = "temp_plot_delta"

def read_timing(timing_file):
    begin_deltas_dict = defaultdict(int)
    end_deltas_dict = defaultdict(int)

    with open(timing_file, 'r') as infile:
        for line in infile:
            if len(line) < 47:
                continue

            # key is the hour:minute field
            key = line[9:14]

            # track two datasets: "Running task" and "Finished task"
            if line[18:45] == "INFO Executor: Running task":
                begin_deltas_dict[key] += 1
                end_deltas_dict[key] += 0
            elif line[18:46] == "INFO Executor: Finished task":
                begin_deltas_dict[key] += 0
                end_deltas_dict[key] += 1
            else:
                pass

    begin_deltas = list()
    end_deltas = list()
    begin_totals = list()
    end_totals = list()
    begin_total = 0
    end_total = 0

    # get the ordered list of keys
    keys = list()
    for k, _ in begin_deltas_dict.items():
        keys.append(k)
    keys.sort()

    # produce deltas and totals
    for k in keys:
        begin_delta = begin_deltas_dict[k]
        end_delta = end_deltas_dict[k]

        begin_deltas.append(begin_delta)
        end_deltas.append(end_delta)

        begin_total += begin_delta
        end_total += end_delta
        begin_totals.append(begin_total)
        end_totals.append(end_total)

        print("key: %s, begin delta: %s, end delta: %s, begin total: %s, end total: %s" % (k, begin_delta, end_delta, begin_total, end_total))
     
    print("begin_deltas: ")
    print(*begin_deltas, sep="\n")
    print("end_deltas: ")
    print(*end_deltas, sep="\n")
    print("begin_totals: ")
    print(*begin_totals, sep="\n")
    print("end_totals: ")
    print(*end_totals, sep="\n")

    return begin_deltas, end_deltas, begin_totals, end_totals

def plot_deltas(begin_deltas, end_deltas, temp_delta):

    # indexes
    indexes = numpy.arange(len(begin_deltas))
    width = 0.3

    fig, ax = plot.subplots()

    rectangles1 = ax.bar(indexes, begin_deltas, width=width, color='r')
    rectangles2 = ax.bar(indexes+width, end_deltas, width=width, color='g')

    ax.set_xlabel("Time in minutes")
    ax.set_title("Task throughput")
    ax.set_ylabel("Tasks per minute")

    plot.savefig(temp_delta)


def plot_totals(begin_totals, end_totals, temp_total):

    # indexes
    indexes = numpy.arange(len(begin_totals))
    width = 0.3

    fig, ax = plot.subplots()

    rectangles1 = ax.bar(indexes, begin_totals, width=width, color='r')
    rectangles2 = ax.bar(indexes+width, end_totals, width=width, color='g')

    ax.set_xlabel("Time in minutes")
    ax.set_title("Task throughput")
    ax.set_ylabel("Tasks processed")

    plot.savefig(temp_total)









if __name__=="__main__":

    parser = ArgumentParser(prog='plot_timestamp.py',
                  description='Plot task timing for collected yarn output')
    parser.add_argument('--timing_file', help= 'timing file',
                        default='bytecount2_timing')
    args = parser.parse_args() 
    timing_file = args.timing_file

    begin_deltas, end_deltas, begin_totals, end_totals = read_timing(
                                                                timing_file)

    plot_deltas(begin_deltas, end_deltas, "temp_deltas.pdf")
    plot_totals(begin_totals, end_totals, "temp_totals.pdf")

