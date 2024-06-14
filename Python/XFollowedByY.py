from collections import defaultdict

data='xyyyyyy'

dictionary= defaultdict(list)
for idx,value in enumerate(data):
    dictionary[value]=idx