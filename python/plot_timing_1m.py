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

time_in_xlabel="Time in minutes"
tasks_per_ylabel="GB per second"

# whole RDC, bytes/splits/1G
gb_per_split = 130018265483776/970124/(1000000000)
seconds_per_bucket = 60

def read_timing(timing_file):
    end_deltas_dict = defaultdict(int)

    with open(timing_file, 'r') as infile:
        for line in infile:
            if len(line) < 47:
                continue

            # key is from hh:mm:ss
            key = line[9:14]    # every minute
#            key = line[9:16]    # every 10 seconds
#            key = line[9:17]    # every 1 second

            # track dataset: "Finished task"
            if line[18:46] == "INFO Executor: Finished task":
                end_deltas_dict[key] += 1
            else:
                pass

    end_deltas = list()
    end_totals = list()
    end_total = 0

    # get the ordered list of keys
    keys = list()
    for k, _ in end_deltas_dict.items():
        keys.append(k)
    keys.sort()

    # produce delta and total lists
    for k in keys:
        end_delta = end_deltas_dict[k] / seconds_per_bucket * gb_per_split

        end_deltas.append(end_delta)

        end_total += end_delta * seconds_per_bucket
        end_totals.append(end_total)

        print("key: %s, end delta: %s, end total: %s" % (k, end_delta, end_total))
     
    print("end_deltas: ")
    print(*end_deltas, sep="\n")
    print("end_totals: ")
    print(*end_totals, sep="\n")

    return end_deltas, end_totals

def plot_deltas(end_deltas, suffix_name):

    # indexes
    indexes = numpy.arange(len(end_deltas))
    width = 0.6

    fig, ax = plot.subplots()

    rectangles1 = ax.bar(indexes, end_deltas, width=width, color='g')

    ax.set_xlabel(time_in_xlabel)
    ax.set_title("Delta GB throughput")
    ax.set_ylabel(tasks_per_ylabel)
    ax.legend((rectangles1[0],), ("Completed",), loc="upper center")

    plot.savefig("temp_deltas_%s.pdf" % suffix_name)


def plot_totals(end_totals, suffix_name):

    # indexes
    indexes = numpy.arange(len(end_totals))
    width = 0.6

    fig, ax = plot.subplots()

    rectangles1 = ax.bar(indexes, end_totals, width=width, color='g')

    ax.set_xlabel(time_in_xlabel)
    ax.set_title("Total GB throughput")
    ax.set_ylabel("Total GB processed")
    ax.legend((rectangles1[0],), ("Completed",), loc="upper center")

    plot.savefig("temp_totals_%s.pdf" % suffix_name)

if __name__=="__main__":

    parser = ArgumentParser(prog='plot_timestamp.py',
                  description='Plot task timing for collected yarn output')
    parser.add_argument('--timing_file', help= 'timing file',
                        default='bytecount2_timing')
    args = parser.parse_args() 
    timing_file = args.timing_file

    end_deltas, end_totals = read_timing(timing_file)

    suffix_name = timing_file
    plot_deltas(end_deltas, suffix_name)
    plot_totals(end_totals, suffix_name)

