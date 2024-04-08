import time
<<<<<<<< HEAD:main_dict.py
from util import extract_datetime, extract_sentiment, draw_result_dict, merge_and_find_max, print_time
========
from util import extract_datetime,extract_sentiment,draw_result, merge_and_find_max, print_time
>>>>>>>> version_array:main_50mb_pv1.py
from mpi4py import MPI
import os

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

FILE = "twitter-50mb.json"

start_time = time.time()

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
# 读取的方法也可以多写 -- generator
with open(FILE, 'r') as file:

    file.seek(position_start,0)

    while True:
        row = file.readline()   #read file
        
        if row == "":
              break
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
<<<<<<<< HEAD:main_dict.py

========
>>>>>>>> version_array:main_50mb_pv1.py
        bytes_already_read +=  len(row.encode('utf-8')) + 1 
        
        if bytes_already_read >= bytes_per_node:
            break

<<<<<<<< HEAD:main_dict.py
========
# print_time(start_time, "Parallel Time (read and process data): ")


# print("My rank is",rank, "My count is", count)

>>>>>>>> version_array:main_50mb_pv1.py

# gather
hd_result_list = comm.gather([day_sentiment_dict], root=0)
hh_result_list = comm.gather([hour_sentiment_dict], root=0)
ad_result_list = comm.gather([day_count_dict], root=0)
ah_result_list = comm.gather([hour_count_dict], root=0)

<<<<<<<< HEAD:main_dict.py
print("################### Parallel Time ###########################")
print_time(start_time, "Parallel Time (read and gather) : ")
========

# print_time(start_time, "Parallel Time (gather data): ")
>>>>>>>> version_array:main_50mb_pv1.py

if rank == 0:
    start_time = time.time()
    # merge and find max
    day_active, max_day_active = merge_and_find_max(ad_result_list)
    hour_active, max_hour_active = merge_and_find_max(ah_result_list)
    day_happy, max_day_happy = merge_and_find_max(hd_result_list)
    hour_happy, max_hour_happy = merge_and_find_max(hh_result_list)
<<<<<<<< HEAD:main_dict.py
    print("################### The answers of four questions ###########################")
    draw_result_dict(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
    print("################### Serial Time ###########################")
    print_time(start_time, "Serial Time (analyse and print): ")
========
    draw_result(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)
    # print_time(start_time, "Serial Time (analyse and print): ")
>>>>>>>> version_array:main_50mb_pv1.py
