"""
    Working with ray

    pip install ray requests beautifulsoup4

    We start ray internally using ray.init(), however, it can be started
    externally and left running indefinitely with

        ray start --head --port 6379

"""
import time

import ray
import requests
from bs4 import BeautifulSoup

ray.init()


# Example1:
@ray.remote
def f(i):
    time.sleep(4)
    return i


start_time = time.time()

results = [f.remote(i) for i in range(4)]

print(results)
print(ray.get(results))
duration = time.time() - start_time
print(duration)


# Example 2
@ray.remote
def f(x):
    time.sleep(1)
    return x


# Start 4 tasks in parallel.
result_ids = [f.remote(i) for i in range(4)]
results = ray.get(result_ids)
print(results)


# Example 3:
@ray.remote
def get_data(url: str):
    try:
        text = requests.get(url).text
        soup = BeautifulSoup(text, 'html.parser')
        result = soup.title.text
    except (TypeError, requests.exceptions.ConnectionError) as err:
        result = err.args[0]

    return result


tasks = ['https://requests.readthedocs.io/en/latest/', 'https://upjoke.com/python-jokes', 'https://pypi.python.org',
         'https://pandas.pydata.org/pandas-docs/stable/', 'http://www.python.org',
         'http://love-python.blogspot.com/', 'http://planetpython.org', 'https://www.python.org/doc/humor/',
         'https://doughellmann.com/blog/', 'https://pymotw.com/3/', 'http://python-history.blogspot.com/',
         'https://nothingbutsnark.svbtle.com/','https://www.pydanny.com/','https://pythontips.com/',
         'http://www.blog.pythonlibrary.org/', 'https://minhhh.github.io/posts/a-guide-to-pythons-magic-methods']

print('Starting jobs...')
obj_refs = [get_data.remote(url) for url in tasks]
results = ray.get(obj_refs)
print(*results, sep='\n')
ray.timeline(filename='timeline.json')
# open Chrome (not FF) to:  chrome://tracing/
# "Load" timeline.json.

if ray.is_initialized:
    ray.shutdown()
    print('Shutting ray down...')
