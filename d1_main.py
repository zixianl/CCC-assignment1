import json
from d1_function import extract_datetime,extract_sentiment,draw_result
## 用dict 存，用array 存可能会更快
day_sentiment_dict  = {}
hour_sentiment_dict  = {}
day_count_dict = {}
hour_count_dict = {}

# 读取的方法也可以多写
with open("twitter-50mb.json", 'r') as file:
    row = file.readline()
    count = 0
    while row != "":
        row = file.readline()
        day, hour = extract_datetime(row)
        sentiment = extract_sentiment(row)
        if day and hour is not None:
            day_count_dict[day] = day_count_dict.get(day, 0) + 1
            hour_count_dict[hour] = hour_count_dict.get(hour, 0) + 1
            if sentiment is not None:
                day_sentiment_dict[day] = day_sentiment_dict.get(day, 0) +  sentiment
                hour_sentiment_dict[hour] = hour_sentiment_dict.get(hour, 0) +  sentiment


day_active = max(day_count_dict, key=lambda k: day_count_dict[k])
hour_active = max(hour_count_dict, key=lambda k: hour_count_dict[k])
day_happy = max(day_sentiment_dict, key=lambda k: day_sentiment_dict[k])
hour_happy = max(hour_sentiment_dict, key=lambda k: hour_sentiment_dict[k])


max_day_active = day_count_dict[day_active]
max_hour_active = hour_count_dict[hour_active]
max_day_happy = day_sentiment_dict[day_happy]
max_hour_happy = hour_sentiment_dict[hour_happy]
draw_result(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy)