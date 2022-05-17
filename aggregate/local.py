import tracemalloc
from billing_functions import gcp, hail, seqr

args = {"attributes": {"start": "2022-04-01", "end": "2022-05-17"}}
gcp()

# tracemalloc.start()
# gcp()
# current, peak = tracemalloc.get_traced_memory()
# print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
# tracemalloc.stop()
