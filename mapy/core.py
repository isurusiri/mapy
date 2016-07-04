import collections
import itertools
import multiprocessing


class MapyCore(object):

    # initializes the mapy core. it expects three arguments
    # map function is a function to map inputs into intermediate
    # data. map function should return a tuple with the key
    # and a value to be reduced
    # reduce function should reduce partitioned intermediate
    # data into final output. Takes a key produced by map
    # function and values correspond to that key
    # number of workers available in the system
    def __init__(self, map_function, reduce_function, no_workers=None):
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.pool = multiprocessing.Pool(no_workers)

    # organizes mapped values by their key and returns tuples with
    # keys and a sequence of values.
    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    # process inputs with map and reduce functions. accepts inputs as an
    # iterable container and the size of a chunk to be given to
    # workers
    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_function, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduce_values = self.pool.map(self.reduce_function, partitioned_data)
        return reduce_values
