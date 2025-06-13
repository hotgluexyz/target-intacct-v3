import ast
import datetime as dt
import json


def parse_objs(record):
    try:
        return json.loads(record)
    except:
        try:
            return ast.literal_eval(record)
        except:
            return record


def dictify(array, key, value):
    array_ = {}
    for i in array:
        array_[i[key]] = i[value]
    return array_


def clean_convert(input):
    if isinstance(input, list):
        return [clean_convert(i) for i in input]
    elif isinstance(input, dict):
        output = {}
        for k, v in input.items():
            v = clean_convert(v)
            if isinstance(v, list):
                output[k] = [i for i in v if i is not None]
            elif v is not None:
                output[k] = v
        return output
    elif isinstance(input, dt.datetime):
        return input.isoformat()
    elif input is not None:
        return input
    
def convert_date(date):
    if date:
        if isinstance(date, dt.datetime):
            date = date.isoformat()
        date = date.split("T")[0]
        date = {
            "year": date.split("-")[0],
            "month": date.split("-")[1],
            "day": date.split("-")[2],
        }
    else:
        date = ""
    return date
