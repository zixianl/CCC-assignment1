import datetime
import time
import numpy as np


def print_time(start_time, time_usage):
    end_time = time.time()
    time_diff = end_time-start_time
    print(time_usage, ": ",time_diff)


def read_jsonFile(file_name):
    with open(file_name, 'r') as file:
        for row in file:
            yield row

## get datetime
def extract_datetime(raw_str):
    start_index = raw_str.find('"created_at":"')
    if start_index == -1:
        return None, None  

    start_index += 14

    datetime_str = raw_str[start_index:start_index+24]

    year = int(datetime_str[0:4])
    month = int(datetime_str[5:7])
    day = int(datetime_str[8:10])
    hour = int(datetime_str[11:13])

    return (year, month, day), hour


# 另一种思路，先找score,再找sentiment,不用call is_float() function
# 坏处： score很少，把每个sentiment都找了一遍，容易报错
# def extract_sentiment(raw_str):
#     start_index = raw_str.find('"score":')
#     if start_index != -1:
#         start_index += 8
#         remaining_str = raw_str[start_index:]
#         print(remaining_str)
#         end_index = remaining_str.find(',')
#         sentiment_str = remaining_str[:end_index].strip()
#         print(sentiment_str)
#         return float(sentiment_str)


#     start_index = raw_str.find('"sentiment":')
#     if start_index == -1:
#         return None  
#     start_index += 12
#     remaining_str = raw_str[start_index:]
#     # print(remaining_str)
#     end_index = remaining_str.find('}')
#     if end_index == -1:  
#         return None
#     sentiment_str = remaining_str[:end_index].strip()
#     ################################
#     # print(sentiment_str)
#     return float(sentiment_str)

def is_float(string):
    if '.' in string:
        parts = string.split('.')
        if len(parts) == 2 and all(part.replace('-', '').isdigit() for part in parts):
            return True
    elif string.startswith('-'):
        return string[1:].isdigit()  
    else:
        return string.isdigit()  
    return False

# # score + sentiment
# def extract_sentiment(raw_str):
#     start_index = raw_str.find('"sentiment":')
#     if start_index == -1:
#         return None
    
#     start_index += 12  

#     remaining_str = raw_str[start_index:]

#     end_index = remaining_str.find('}')
#     if end_index == -1:
#         return None
    

#     sentiment_str = remaining_str[:end_index].strip()

#     if is_float(sentiment_str):
#         return float(sentiment_str)


#     start_index_score = sentiment_str.find('"score":')
#     if start_index_score != -1:
#         start_index_score += 8 
#         end_index_score = sentiment_str.find(',', start_index_score)
#         score_str = sentiment_str[start_index_score:end_index_score].strip()
#         if is_float(score_str):
#             return float(score_str)
    
#     return None

# get sentiment
def extract_sentiment(raw_str):
    start_index = raw_str.find('"sentiment":')
    if start_index == -1:
        return None
    
    start_index += 12  

    remaining_str = raw_str[start_index:]

    end_index = remaining_str.find('}')
    if end_index == -1:
        return None
    
    sentiment_str = remaining_str[:end_index].strip()

    if is_float(sentiment_str):
        return float(sentiment_str)
    return None





## ===========functions for processing dict data===========
def convert_to_readable_date(date_tuple):
    date = datetime.datetime(date_tuple[0], date_tuple[1], date_tuple[2])
    return date.strftime("%dth %b %Y")

def convert_to_ampm(hour):
    if hour == 0:
        return "12am"
    elif hour < 12:
        return str(hour) + "am"
    elif hour == 12:
        return "12pm"
    else:
        return str(hour - 12) + "pm"
    
def convert_to_sentiment_score(score):
    if score > 0:
        score = round(score, 2)
        result_str = "+" + str(score)
        return result_str
    return result_str

def format_output(hour, day, count_or_score):
    day_output = convert_to_readable_date(day)
   
    if hour is None:
        if isinstance(count_or_score, int):
     
            return f"{day_output} had the most tweets (#{count_or_score})"

        return f"{day_output} was the happiest day with an overall sentiment score of {convert_to_sentiment_score(count_or_score)}"
    

    time_range = f"{convert_to_ampm(hour)} - {convert_to_ampm(hour+1)}"
    if isinstance(count_or_score, int):
        return f"{time_range} on {day_output} had the most tweets (#{count_or_score})"
    

    return f"{time_range} on {day_output} was the happiest hour with an overall sentiment score of {convert_to_sentiment_score(count_or_score)}"


def draw_result_dict(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy):

    print(format_output(hour = hour_happy[1], day = hour_happy[0], count_or_score = max_hour_happy))
    print(format_output(hour = None, day = day_happy, count_or_score = max_day_happy))
    print(format_output(hour = hour_active[1], day = hour_active[0], count_or_score = int(max_hour_active)))
    print(format_output(hour = None, day = day_active, count_or_score = int(max_day_active)))


    


def merge_and_find_max(results_list):
    merged_dict = {}
    for result in results_list:
        for sub_dict in result:
            for key, value in sub_dict.items():
                if key in merged_dict:
                    merged_dict[key] += value
                else:
                    merged_dict[key] = value
    
    max_key = max(merged_dict, key=lambda k: merged_dict[k])
    max_value = merged_dict[max_key]
    
    return max_key, max_value


## json.load
# def extract_data(row):
#     row_dict = json.load(row)
#     date_str = row_dict["rows"][0]["doc"]["data"]["created_at"][:10]
#     date_obj = datetime.strptime(date_str, "%Y-%m-%d")
#     date_tuple = (date_obj.year, date_obj.month, date_obj.day)
#     sentiment_str = row_dict["rows"][0]["doc"]["data"]["sentiment"]
#     if sentiment_str.isnumeric():
#         sentiment_row = float(sentiment_str)
#     else:
#         sentiment_row = None
#     return date_tuple, sentiment_row



## ===========functions for processing array data===========
# find the max value with its index of array: 
# aim to get the happiest hour and the most active hour
# input: numpy array
# return max value with index
def get_max(arr):
    max_index_flat = np.argmax(arr)
    max_index = np.unravel_index(max_index_flat, arr.shape)
    max_value = arr[max_index]
    return max_index, max_value


# find the max value with its index after summing along the 3rd dimension: 
# aim to get the happiest day and the most active day
# input: numpy array
# return max value with index 
def get_max_sum(arr):
    summed_arr = np.sum(arr, axis=3) # summing along days
    max_index_flat = np.argmax(summed_arr)
    max_index = np.unravel_index(max_index_flat, summed_arr.shape)
    max_value = summed_arr[max_index]
    return max_index, max_value


# convert the date tuple into readable datetime string
def convert(input_tuple):
    # since we store data in a way that program reads
    # first turn the data into readable ways
    YEAR_START = 2020
    date_list = list(input_tuple)
    date_list[0] += YEAR_START
    date_list[1] += 1
    date_list[2] += 1
    date_tuple = tuple(date_list)

    if len(date_tuple) == 4:
        date = datetime.datetime(year=date_tuple[0], month=date_tuple[1], day=date_tuple[2], hour=date_tuple[3])
        readable_datetime = date.strftime("%B %d, %Y, %-I%p")
    else:
        date = datetime.date(year=date_tuple[0], month=date_tuple[1], day=date_tuple[2])
        readable_datetime = date.strftime("%B %d, %Y")

    return readable_datetime



def draw_result_arr(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy):
    print("The most happiest hour is:", convert(hour_happy), "with the overall sentiment score", max_hour_happy)
    print("The most happiest day is:", convert(day_happy), "with the overall sentiment score", max_day_happy)
    print("The most active hour is:", convert(hour_active), "with the overall count", max_hour_active)
    print("The most active day is:", convert(day_active), "with the overall count", max_day_active)