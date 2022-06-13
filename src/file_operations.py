import os
from typing import List
import datetime as dt
from numpy import save

METRICS_FOLDER_PATHNAME = '\\'.join(os.path.dirname(__file__).split('\\')[:-1])+'\\'+'data'+'\\'+'metrics.txt'
# print(DATA_FOLDER_PATHNAME)

def save_to_file(pathname, header, data):
    with open(pathname, 'a', encoding='utf-8') as f:
        f.write(80*'-'+'\n')
        f.write(f'Time of execution: {dt.datetime.now()}\n')
        f.write(header)
        f.write('\n')
        if isinstance(data, list):
            for elem in data:
                f.write(str(elem)+'\n')
        else:
            f.write(str(data)+'\n')
        f.write(80*'-'+'\n')


# save_to_file(METRICS_FOLDER_PATHNAME, 'test', 'blabla')

# f = io.open(DATA_FOLDER_PATHNAME, mode="r", encoding="utf-8")
# print(f.read())