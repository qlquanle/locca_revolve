import os
import sys
import numpy as np
import pandas as pd
import requests
# from tqdm.notebook import tqdm as tqdm
from tqdm import tqdm
from functools import partial
from pathlib import Path
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
import errno
import time
import shutil
import socket

clear_files=True
n_workers=10
chunk_size=10

with open('input/user_agents.txt', 'r') as f:
    user_agents = f.readlines()
user_agents = [x.strip() for x in user_agents]


# HELPERS
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python > 2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

log_file = './logs/log_%s.txt' % time.strftime("%Y%m%d-%H%M%S")
def write_to_log(m):
    print(m)
    with open(log_file, 'a+') as f:
        f.write(m + "\n")
    return None


def parallelize_download(chunk):
    for i in range(len(chunk)):
        HEADERS = {
            'User-Agent': np.random.choice(user_agents)
        }
        out_folder = '/'.join(chunk[i][1].split('/')[:-1])
        mkdir_p(out_folder)
        r = requests.get(chunk[i][0], headers=HEADERS)
        with open(chunk[i][1], 'wb') as f:
            f.write(r.content)
    return 0          

def list_all_files(path, ends, ends_only=True):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for filename in f:
            if ends in filename:
                if not ends_only:
                    filename = os.path.join(r, filename)
                files.append(filename)
    return files
# HELPERS

def main():
    with open('input/test_urls.txt', 'r') as f:
        test_urls = f.readlines()
    test_urls = [x.strip() for x in test_urls]
    in_urls = test_urls

    url_prefix = 'https://chroniclingamerica.loc.gov/data/batches/'
    batch_names = [x.replace(url_prefix, '').split('/')[0] for x in test_urls]
    url_tails = [x.replace(url_prefix, '').split('/')[1:] for x in test_urls]
    url_tails = ['/'.join(x) for x in url_tails]

    out_path = "./scans/"
    out_files = [x.replace(url_prefix, out_path) for x in test_urls]
    if clear_files:
        shutil.rmtree(out_path)

    in_zip = list(zip(in_urls, out_files))
    my_chunks = list(chunks(in_zip, chunk_size))
    
    write_to_log('Host name %s' % socket.gethostname())
    write_to_log('Test batch size: %s' % len(test_urls))
    write_to_log('Chunk size: %s' % chunk_size)
    write_to_log('Number of workers: %s' % n_workers)

    pool = ThreadPool(n_workers)
    time0 = time.time()
    results = list(tqdm(pool.imap(parallelize_download, my_chunks)))
    time1 = time.time()
    time_elapsed=(time1-time0)

    write_to_log('Time elapsed: %s secs' % round(time_elapsed))

    time_for_remaining = 15000000 * (time_elapsed/len(test_urls))
    write_to_log('Expected download time for 15m scans: %s secs' % round(time_for_remaining))
    write_to_log('                                     (%s hours)' % round(time_for_remaining / 3600))

    list_of_files = list_all_files(out_path, '.jp2')
    write_to_log('Files downloaded: %s' % len(list_of_files))
    return None

if __name__ == "__main__":
    main()