import datetime


def benchmark(func):
    import time

    def wrapper(*args, **kwargs):
        name = func.__name__
        start = time.time()
        return_value = func(*args, **kwargs)
        end = time.time()
        print(f'{name} Время выполнения: {end - start} секунд.')
        return return_value

    return wrapper


def limit_interp(func, min_x, max_x, min_y):
    def wrapper(x, y):
        if x is None or y is None:
            return
        if type(x) in (int, float):
            if x < min_x:
                x = min_x
            elif x > max_x:
                x = max_x
            if y < min_y:
                y = min_y

        else:
            x[x < min_x] = min_x
            x[x > max_x] = max_x
        return func(x, y)

    return wrapper


def my_float(_str):
    try:
        return float(_str)
    except:
        return


def pars_date(str_date):
    # date = datetime.datetime.strptime(str_date, '%d.%m.%Y')
    if len(str_date.split('.')) > 1:
        str_date = '.'.join(str_date.replace('.', ' ').split())
        try:
            date = datetime.datetime.strptime(str_date, '%d.%m.%Y').date()
            year = date.year
        except:
            date = None
            try:
                year = int(str_date.split('.')[-1])
            except:
                year = None
    else:
        date = None
        try:
            year = datetime.datetime.strptime(str_date, '%Y').year
        except:
            year = None
    return date, year


def pars_crew(str_in, crew_n):
    if str_in != '':
        if len(str_in.split('')) > crew_n:
            out_str = str_in.split('')[crew_n]
        else:
            out_str = None
    else:
        out_str = None
    return out_str

def progress_bar(iteration, total, prefix='', suffix='', length=30, fill='█'):
    percent = "{:.1f}".format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end='\r')
    # Print a new line when the progress is complete
    if iteration == total:
        print()


if __name__ == '__main__':
    pass
