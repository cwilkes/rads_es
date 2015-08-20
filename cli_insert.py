import sys
import argparse
from elasticsearch import Elasticsearch
import gzip


class GzipAwareFileType(argparse.FileType):
    """Used to read in both text and .gz files"""
    def __call__(self, string):
        if string.endswith('.gz'):
            return gzip.open(string, self._mode, self._bufsize)
        else:
            return super(GzipAwareFileType, self).__call__(string)


def rads_converter():
    """
    :return:function that converts from a rads line of uid (tab) ns,site,cat,freq,rec (space) ns,site... (space) endvalue
    into a json object to put into ES
    """
    def convert(line):
        p = line.split()
        attributes = list()
        for e in (_.split(',') for _ in p[1:-1]):
            attributes.append(dict(ns=int(e[0]), site=int(e[1]), cat=int(e[2]), frequency=int(e[3]), recency=int(e[4])))
        return dict(_id=int(p[0]), attributes=attributes)
    return convert


def split_reader(reader, chunk_size=5000):
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

    def add_elements(self, reader, converter=None):
        print 'working on', reader
        if converter is None:
            converter = rads_converter()
        reader = (converter(_.strip()) for _ in reader)
        reader = (dict(index=_) for _ in reader)
        while True:
            inserts = self.es.bulk(split_reader(reader, 1000), index='rads', doc_type='user')
            number_inserts = len(inserts['items'])
            if number_inserts == 0:
                break
            print 'inserted', number_inserts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str, help='elasticsearch host')
    parser.add_argument('files', type=GzipAwareFileType('r'), nargs='+')
    args = parser.parse_args()

    inserter = Inserter(args.host)
    for fn in args.files:
        inserter.add_elements(fn)


if __name__ == '__main__':
    main();