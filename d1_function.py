import json
import datetime

def read_jsonFile(file_name):
    with open(file_name, 'r') as file:
        for row in file:
            yield row
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

# sentiment
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

    

def convert_to_ampm(hour):
    if hour == 0:
        return "12am"
    elif hour < 12:
        return str(hour) + "am"
    elif hour == 12:
        return "12pm"
    else:
        return str(hour - 12) + "pm"


def convert_to_readable_date(date_tuple):
    date = datetime.datetime(date_tuple[0], date_tuple[1], date_tuple[2])
    return date.strftime("%dth %b %Y")

# 输出结果
def draw_result(day_active,hour_active,day_happy,hour_happy,max_day_active,max_hour_active,max_day_happy,max_hour_happy):
    print("The most active day is:", convert_to_readable_date(day_active), "with the overall count", max_day_active)
    print("The most active hour is:", convert_to_ampm(hour_active), "with the overall count", max_hour_active)
    print("The most happiest day is:", convert_to_readable_date(day_happy), "with the overall sentiment score", max_day_happy)
    print("The most happiest hour is:", convert_to_ampm(hour_happy), "with the overall sentiment score", max_hour_happy)

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