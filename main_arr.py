import time
from util import extract_datetime, extract_sentiment, print_time, get_max, get_max_sum, draw_result_arr
import os
import numpy as np
from mpi4py import MPI

start_time = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

FILE = "twitter-1mb.json"

## store with 4D array
# Asumme recent 5 years interested
YEAR_START = 2020
YEAR_END = 2024
NUM_YEARS = YEAR_END - YEAR_START + 1
SHAPE = (NUM_YEARS, 12, 31, 24) #year,mon,day,hour
hour_sentiment_arr = np.zeros(shape=SHAPE, dtype=float)
hour_count_arr = np.zeros(shape=SHAPE, dtype=int)


total_bytes = os.path.getsize(FILE)
bytes_per_node = total_bytes // size
bytes_already_read = 0
position_start = rank * bytes_per_node
count = 0


## read with 3D array
with open(FILE, 'r') as file:
    file.seek(position_start,0)
    while True:
        row = file.readline()

        if row == "":
            break
        if rank == 0 or bytes_already_read > 0:
            count += 1
            date, hour = extract_datetime(row)   # (year, month, day), hour --> date, hour
            sentiment = extract_sentiment(row)   # float

            if date and hour is not None:
                year_index = date[0] - YEAR_START
                hour_count_arr[year_index, date[1]-1, date[2]-1, hour] += 1
                if sentiment is not None:
                    hour_sentiment_arr[year_index, date[1]-1, date[2]-1, hour] += sentiment
            
        bytes_already_read +=  len(row.encode('utf-8')) + 1 
        if bytes_already_read >= bytes_per_node:
            break


# print_time(start_time, "Parallel Time (read and process data): ")
# print("My rank is",rank, "My count is", count)   


# reduce
hour_sentiment_gathered = comm.reduce(hour_sentiment_arr, op=MPI.SUM, root=0)
hour_count_gathered = comm.reduce(hour_count_arr, op=MPI.SUM, root=0)

print("################### Parallel Time ###########################")
print_time(start_time, "Parallel Time (read and gather) : ")

if rank == 0:
    hour_happy, max_hour_happy = get_max(hour_sentiment_gathered)
    hour_active, max_hour_active = get_max(hour_count_gathered)
    day_happy, max_day_happy = get_max_sum(hour_sentiment_gathered)
    day_active, max_day_active = get_max_sum(hour_count_gathered)

    print("################### The answers of four questions ###########################")  
    draw_result_arr(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
    print("################### Serial Time ###########################")
    print_time(start_time, "Serial Time (analyse and print): ")




'''
# gather
hour_sentiment_gathered = comm.gather(hour_sentiment_arr, root=0)
hour_count_gathered = comm.gather(hour_count_arr, root=0)

# print_time(start_time, "Parallel Time (read and gather) : ")

if rank == 0:
    hour_sentiment_summed = np.sum(np.array(hour_sentiment_gathered), axis=0)
    hour_count_summed = np.sum(np.array(hour_count_gathered), axis=0)

    hour_happy, max_hour_happy = get_max(hour_sentiment_summed)
    hour_active, max_hour_active = get_max(hour_count_summed)
    day_happy, max_day_happy = get_max_sum(hour_sentiment_summed)
    day_active, max_day_active = get_max_sum(hour_count_summed)

    print("################### The answers of four questions ###########################")
    draw_result_arr(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
    print("################### Serial Time ###########################")
    print_time(start_time, "Serial Time (analyse and print): ")
'''