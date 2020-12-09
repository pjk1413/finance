

def lowest_value(list, candle_value):
    min = getattr(list[0], candle_value)
    for x in list:
        if getattr(x, candle_value) < min:
            min = getattr(x, candle_value)
    return min


def highest_value(list, candle_value):
    max = getattr(list[0], candle_value)
    for x in list:
        if getattr(x, candle_value) > max:
            max = getattr(x, candle_value)
    return max

def simple_moving_average(list, period):
    pass

def avg(list):
    avg = (sum(list)/len(list))
    return avg

def smoothed_avg(list):
    smoothed_avg = (sum(list[1:])/(len(list)-1) * ((len(list) - 1)/len(list)) + list[0])/len(list)
    # smoothed_avg = ((sum(list[1:])/(len(list)-1) * (len(list) - 1)/len(list)) + list[0])/len(list)
    return smoothed_avg