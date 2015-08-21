import sys
import argparse
from elasticsearch import Elasticsearch
import gzip
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from datetime import datetime
from elasticsearch.helpers import bulk

class GzipAwareFileType(argparse.FileType):
    """Used to read in both text and .gz files"""
    def __call__(self, string):
        if string.endswith('.gz'):
            return gzip.open(string, self._mode, self._bufsize)
        else:
            return super(GzipAwareFileType, self).__call__(string)


def rads_converter(date):
    """
    :return:function that converts from a rads line of uid (tab) ns,site,cat,freq,rec (space) ns,site... (space) endvalue
    into a json object to put into ES
    """
    recency_map = dict()
    for recency in range(40):
        recency_map[recency] = (date+relativedelta(days=-recency)).strftime('%Y-%m-%d')

    def convert(line):
        p = line.split()
        attributes = list()
        for e in (_.split(',') for _ in p[1:-1]):
            attributes.append(dict(ns=int(e[0]), site=int(e[1]), cat=int(e[2]), frequency=int(e[3]), date=recency_map[int(e[4])]))
        return dict(uid=long(p[0]), tags=attributes)
    return convert


def split_reader(reader, chunk_size=1000):
    i = 0
    while i < chunk_size:
        try:
            yield next(reader)
        except:
            break
        i += 1


class Inserter(object):
    def __init__(self, host):
        if host == '-':
            self.es = Elasticsearch()
        else:
            self.es = Elasticsearch(host)

    def add_elements(self, reader, date, converter=None):
        print 'working on', reader
        if converter is None:
            converter = rads_converter(date)
        reader = (converter(_.strip()) for _ in reader)
        total_inserts = 0
        while True:
            inserts = bulk(self.es, [{'_index': 'rads', '_type': 'profile', '_source': _} for _ in split_reader(reader, 1000)])
            number_inserts = inserts[0]
            if number_inserts == 0:
                break
            total_inserts += number_inserts
            print datetime.now(), 'inserted', number_inserts, total_inserts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str, help='elasticsearch host')
    parser.add_argument('date', type=str, help='date of file')
    parser.add_argument('files', type=GzipAwareFileType('r'), nargs='+')
    args = parser.parse_args()

    inserter = Inserter(args.host)
    for fn in args.files:
        inserter.add_elements(fn, parse(args.date) + relativedelta(days=-2))


if __name__ == '__main__':
    main();