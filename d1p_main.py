import json
import time
from d1_function import extract_datetime,extract_sentiment,draw_result, merge_and_find_max
from mpi4py import MPI
import os


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


start_time = time.time()

## 用dict 存，用array 存可能会更快
day_sentiment_dict  = {}
hour_sentiment_dict  = {}
day_count_dict = {}
hour_count_dict = {}

total_bytes = os.path.getsize("twitter-50mb.json")
bytes_per_node = total_bytes // size
bytes_already_read = 0
position_start = rank * bytes_per_node
count = 0
# 读取的方法也可以多写
with open("twitter-50mb.json", 'r') as file:

    file.seek(position_start,0)

    while (row := file.readline())  != "":
       
        if bytes_already_read > bytes_per_node:
            break

        day, hour = extract_datetime(row)
        sentiment = extract_sentiment(row)
        if day and hour is not None:
            day_count_dict[day] = day_count_dict.get(day, 0) + 1
            hour_count_dict[hour] = hour_count_dict.get(hour, 0) + 1
            if sentiment is not None:
                day_sentiment_dict[day] = day_sentiment_dict.get(day, 0) +  sentiment
                hour_sentiment_dict[hour] = hour_sentiment_dict.get(hour, 0) +  sentiment
        bytes_already_read += len(row)
        count += 1 
end_time = time.time()


print("My rank is",rank, "My count is", count)

hd_result_list = comm.gather([day_sentiment_dict], root=0)
hh_result_list = comm.gather([hour_sentiment_dict], root=0)
ad_result_list = comm.gather([day_count_dict], root=0)
ah_result_list = comm.gather([hour_count_dict], root=0)

if rank == 0:
    day_active, max_day_active = merge_and_find_max(ad_result_list)
    hour_active, max_hour_active = merge_and_find_max(ah_result_list)
    day_happy, max_day_happy = merge_and_find_max(hd_result_list)
    hour_happy, max_hour_happy = merge_and_find_max(hh_result_list)
    draw_result(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)