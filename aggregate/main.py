import tracemalloc
from billing_functions import gcp, hail, seqr

args = {"attributes": {"start": "2022-05-01", "end": "2022-05-16"}}
gcp(args)

# tracemalloc.start()
# gcp()
# current, peak = tracemalloc.get_traced_memory()
# print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
# tracemalloc.stop()
