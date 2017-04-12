#!/usr/bin/env python
# Calculate flex bytes considered and accepted from the log file.
from argparse import ArgumentParser

def read_totals(logfile):
    flex_bytes_considered = 0
    flex_bytes_accepted = 0

    with open(logfile, 'r') as infile:
        for line in infile:
            if line[:23] == "flex_bytes_considered: ":
                flex_bytes_considered += int(line[23:])
            elif line[:21] == "flex_bytes_accepted: ":
                flex_bytes_accepted += int(line[21:])

    return flex_bytes_considered, flex_bytes_accepted


if __name__=="__main__":

    parser = ArgumentParser(prog='calculate_flex_bytes.py',
                  description='Calculate flex bytes considered and accepted from the log file')
    parser.add_argument('--logfile', help= 'Yarn log file',
                        default='w')
    args = parser.parse_args() 
    logfile = args.logfile

    flex_bytes_considered, flex_bytes_accepted = read_totals(logfile)

    print("flex_bytes_considered: %d" % flex_bytes_considered)
    print("flex_bytes_accepted: %d" % flex_bytes_accepted)

