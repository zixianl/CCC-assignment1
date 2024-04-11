import time
from util import extract_datetime, extract_sentiment, print_time, get_max, get_max_sum, draw_result_arr
import os
import numpy as np
from mpi4py import MPI

start_time = time.time()
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

FILE = "twitter-100gb.json"
## =========== method for storing data in array ===========
# Asumme only interested in recent 5 years
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

        # This is to let the rank exclude rank 0 to ignore the first line
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



# reduce method
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


# # gather method
# hour_sentiment_gathered = comm.gather(hour_sentiment_arr, root=0)
# hour_count_gathered = comm.gather(hour_count_arr, root=0)

# print("################### Parallel Time ###########################")
# print_time(start_time, "Parallel Time (read and gather) : ")

# if rank == 0:
#     hour_sentiment_summed = np.sum(np.array(hour_sentiment_gathered), axis=0)
#     hour_count_summed = np.sum(np.array(hour_count_gathered), axis=0)

#     hour_happy, max_hour_happy = get_max(hour_sentiment_summed)
#     hour_active, max_hour_active = get_max(hour_count_summed)
#     day_happy, max_day_happy = get_max_sum(hour_sentiment_summed)
#     day_active, max_day_active = get_max_sum(hour_count_summed)

#     print("################### The answers of four questions ###########################")
#     draw_result_arr(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
#     print("################### Serial Time ###########################")
#     print_time(start_time, "Serial Time (analyse and print): ")


## =========== method for storing data in dict ===========
'''
from util import extract_datetime, extract_sentiment, draw_result_dict, merge_and_find_max, print_time
## store with dict
day_sentiment_dict  = {}
hour_sentiment_dict  = {}
day_count_dict = {}
hour_count_dict = {}


total_bytes = os.path.getsize(FILE)
bytes_per_node = total_bytes // size
bytes_already_read = 0
position_start = rank * bytes_per_node
count = 0


## read with dict
with open(FILE, 'r') as file:

    file.seek(position_start,0)

    while True:
        row = file.readline()   #read file
        
        if row == "":
              break
        
        # This is to let the rank exclude rank 0 to ignore the first line
        if rank == 0 or bytes_already_read > 0:
            count += 1
            day, hour = extract_datetime(row)
            sentiment = extract_sentiment(row)

            if day and hour is not None:
                day_count_dict[day] = day_count_dict.get(day, 0) + 1
                hour_count_dict[(day,hour)] = hour_count_dict.get((day,hour), 0) + 1
                if sentiment is not None:
                    day_sentiment_dict[day] = day_sentiment_dict.get(day, 0) +  sentiment
                    hour_sentiment_dict[(day,hour)] = hour_sentiment_dict.get((day,hour), 0) +  sentiment

        bytes_already_read +=  len(row.encode('utf-8')) + 1 
        
        if bytes_already_read >= bytes_per_node:
            break


# gather
hd_result_list = comm.gather([day_sentiment_dict], root=0)
hh_result_list = comm.gather([hour_sentiment_dict], root=0)
ad_result_list = comm.gather([day_count_dict], root=0)
ah_result_list = comm.gather([hour_count_dict], root=0)

print("################### Parallel Time ###########################")
print_time(start_time, "Parallel Time (read and gather) : ")

start_time = time.time()

if rank == 0:
    # merge and find max
    day_active, max_day_active = merge_and_find_max(ad_result_list)
    hour_active, max_hour_active = merge_and_find_max(ah_result_list)
    day_happy, max_day_happy = merge_and_find_max(hd_result_list)
    hour_happy, max_hour_happy = merge_and_find_max(hh_result_list)
    print("################### The answers of four questions ###########################")
    draw_result_dict(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
    print("################### Serial Time ###########################")
    print_time(start_time, "Serial Time (analyse and print): ")
'''