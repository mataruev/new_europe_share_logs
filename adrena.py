from collections import namedtuple
from datetime import datetime
import os
import multiprocessing
from tqdm import tqdm

import pandas as pd
from dateutil import parser

# import chardet

from zipping_files import un_gzip_to_memory
from dif_func import progress_bar, benchmark

Field = namedtuple('Field', ['number', 'long_name', 'middle_name', 'short_name', 'some_1', 'units', 'some_2',
                             'some_3'])


def divide_list_into_sublists(lst, sublist_size):
    divided_list = []
    for i in range(0, len(lst), sublist_size):
        sublist = lst[i:i + sublist_size]
        divided_list.append(sublist)
    return divided_list


def pars_lat_lon(value: str) -> float:
    degrees = int(value[:-8])
    minutes = float(value[-8:-2])
    res = degrees + minutes / 60
    return res


class AdrenaTrack:

    @staticmethod
    def pars_utc_date(v):
        return parser.parse(v[0:v.find(' ')], dayfirst=True).date()

    @staticmethod
    def parse_date(v):
        return parser.parse(v, dayfirst=True).date()

    @staticmethod
    def parse_lat_lon(v):
        degrees = int(v[:-8])
        minutes = float(v[-8:-2])
        res = degrees + minutes / 60
        return res

    def parse_utc_time(self, v):
        return datetime.strptime(v[v.find(' ') + 1:], self.time_format).time()

    def parse_local_time(self, v):
        return datetime.strptime(v, self.time_format).time()

    def parse_lat(self, v):
        return -self.parse_lat_lon(v) if v[-1:] in ("S", "s") else self.parse_lat_lon(v)

    def parse_lon(self, v):
        return -self.parse_lat_lon(v) if v[-1:] in ("W", "w") else self.parse_lat_lon(v)

    def __init__(self, inp_file_name: str, out_path: str = ""):
        self.time_format = "%H:%M:%S"
        self.linesep = '\n'
        self.start_index_xdr_fields = 47
        self.out_path = out_path
        self.inp_file_name: str = inp_file_name
        file_name = os.path.basename(self.inp_file_name)
        file_name_without_extension = os.path.splitext(file_name)[0]
        self.out_file = os.path.join(out_path, file_name_without_extension + '.csv')
        self.inp_file_name = inp_file_name
        self.xdr_fields = []
        # self.fields_pos = dict(local_date=1, local_time=1, lat=(3, 4), lon=(5, 6), sog=8, cog=10, bsp=12,
        #                        heading_true=14,
        #                        twd=16,
        #                        awa=18, aws=20, twa=22, tws=24, depth=26, vmg=28, utc_date=31, utc_time=32,
        #                        atm_pressure=38,
        #                        air_temp=40, water_temp=42, cur_speed=229, cur_dir=230, tide_height=236,
        #                        tide_percent=237,
        #                        altitude=47, hdop=51, vdop=53, pdop=55, geoidal_seperation=57, pos_quality=59,
        #                        pos_integrity=61, sats_view=63, sdgps_status=65, log=67, trip1_dist=69, trip1_time=71,
        #                        bsp2=77,
        #                        trip1_avg_spd=79, trip1_max_spd=81, rot=83, trim=85, mag_var=87, rudder=89,
        #                        commanded_ruder=91,
        #                        gps_fix_type=93, depth_offset=95, course=97, heading_opp_tack=99, keel_angle=101,
        #                        leeway=103,
        #                        mast_angle=105, target_twa=107, race_timer=109, target_bsp=111, vmg_2=113,
        #                        polar_bsp=115,
        #                        polar_perf=117, wa_to_mast=119, bow_lat=121, bow_lon=123, dead_reckon_bearing=125,
        #                        dead_reckon_dist=127, heel=129, mwa=131, mws=133, optimum_wa=135, pith_rate=137)
        self.int_fields = (
            'cog', 'heading_true', 'twd', 'awa', 'twa', 'cur_dir', 'tide_percent', 'pos_quality', 'pos_integrity',
            'sats_view', 'sdgps_status', 'gps_fix_type')

        self.static_fields_pos = dict(utc_date=1, utc_time=1, lat=(3, 4), lon=(5, 6), sog=8, cog=10, bsp=12,
                                      heading_true=14, twd=16,
                                      awa=18, aws=20, twa=22, tws=24, depth=26, vmg=28, local_date=31, local_time=32,
                                      atm_pressure=38,
                                      air_temp=40, water_temp=42, cur_speed=229, cur_dir=230, tide_height=236,
                                      tide_percent=237)
        self.conversion_map = {
            'utc_date': self.pars_utc_date,
            'utc_time': self.parse_utc_time,
            'lat': self.parse_lat,
            'lon': self.parse_lon,
            'local_date': self.parse_date,
            'local_time': self.parse_local_time,
        }
        self.xdr_fields = dict()
        if self.inp_file_name.endswith("trz") | self.inp_file_name.endswith("jtz"):
            self.text: str = self.read_track_from_trz()
        elif self.inp_file_name.endswith("trc"):
            self.text: str = self.read_track_from_trc()
        else:
            print("Unknown file type!")
            exit(100)
        self.xdr_headers()

    def read_track_from_trz(self) -> str | None:
        try:
            bytes_values = un_gzip_to_memory(self.inp_file_name)
            # result = chardet.detect(bytes_values)
            # encoding = result['encoding']
            encoding = 'Latin-1'
            txt_file = bytes_values.decode(encoding)

        except Exception as e:
            print(e)
            return
        return txt_file

    def read_track_from_trc(self) -> str | None:
        try:
            with open(self.inp_file_name, "rb") as f:
                bytes_values = f.read()
            # result = chardet.detect(bytes_values)
            # encoding = result['encoding']
            encoding = 'Latin-1'
            txt_file = bytes_values.decode(encoding)

        except Exception as e:
            print(e)
            return
        return txt_file

    # def just_investigate(self, show: bool):
    #     """ Attempt to figure out fields in trz file"""
    #     field = "499,Pilot Gust Bear Away,Pilot Gust,GBA,2,°,9,10"
    #     _, *sentences = description_str.split(",")
    #     sublists = divide_list_into_sublists(sentences, 8)
    #     self.dynamic_fields = [Field(l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7]) for l in sublists]
    #     _, _, *recs = data_str.split(',')
    #     count = 0
    #
    #     for rec in recs:
    #         print(count, rec, sep='/t')
    #         count += 1
    #     print(fields)

    def xdr_headers(self, show: bool = False):
        raw_lines = self.text.split(self.linesep)
        for line in raw_lines:
            if line[0:line.find(',')] == 'VarXdr':
                _, *sentences = line.split(",")
                sublists = divide_list_into_sublists(sentences, 8)
                self.xdr_fields = [Field(lc[0], lc[1], lc[2], lc[3], lc[4], lc[5], lc[6], lc[7]) for lc in sublists]
                if show:
                    for ind, field in enumerate(self.xdr_fields):
                        print(ind, field)
                break

    def show_all_fields_index(self, lines_number):

        raw_lines = self.text.split(self.linesep)
        lines = []
        for line in raw_lines:
            if line[0:line.find(',')] == '$TANAV':
                lines.append(line)
        for ind, line in enumerate(lines):
            if ind == lines_number - 1:
                for index, value in enumerate(line.split(',')):
                    print(index, value, sep='/t')
                break

    def pars_row_data(self, data_line: str) -> dict:
        data = data_line.split(',')

        row = dict()
        for key, value in self.static_fields_pos.items():
            if type(value) == tuple:
                val = ""
                for i in value:
                    val += ' ' + data[i]
            else:
                if value >= len(data) - 1:
                    continue
                val = data[value]
            value = self.pars_field(key, val)
            # if value is None:
            #     print(f"Line {data[1]} ")
            row[key] = value
        for index, field in enumerate(self.xdr_fields):
            position = self.start_index_xdr_fields + index * 2
            if field.short_name in row.keys() or field.short_name == '' or position + 1 >= len(data) - 1:
                continue

            if data[position + 1] in ('N', ''):
                row[field.short_name] = None
            else:
                row[field.short_name] = self.pars_field(field.short_name, data[position])
        return row

    def pars_field(self, field_name, value, verbose: bool = False):
        default_func = (lambda v: int(v)) if field_name in self.int_fields else (lambda v: float(v))
        try:
            conversion_func = self.conversion_map.get(field_name, default_func)
            parsed = conversion_func(value)
            return parsed
        except (ValueError, TypeError) as e:
            if verbose:
                print(f"{field_name} can't be converted")
            return None

    def trz_parsing(self, tasks: int, show_progress: bool):
        raw_lines = self.text.split(self.linesep)
        lines = []
        for line in raw_lines:
            if line[0:line.find(',')] == '$TANAV':
                lines.append(line)
        if tasks > 0:
            pool = multiprocessing.Pool(processes=tasks)

            if show_progress:
                chunk_size = 1000  # Number of lines per chunk
                chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]
                total_chunks = len(chunks)

                # Initialize the progress bar
                progress_bar_tq = tqdm(total=total_chunks * chunk_size, desc='Progress')
                parsed_results = []
                for chunk in chunks:
                    chunk_results = pool.map(self.pars_row_data, chunk)
                    parsed_results.extend(chunk_results)
                    progress_bar_tq.update(chunk_size)  # Update the progress bar for each chunk

                progress_bar_tq.close()
                # *********************************************************************
            else:
                # With out progress bar **********************
                parsed_results = pool.map(self.pars_row_data, lines)
                # *************************************************************
            pool.close()
            pool.join()
        else:
            # VAR consecutive *****************************************
            parsed_results = []
            total = len(lines)
            for ind, line in enumerate(lines):
                parsed_results.append(self.pars_row_data(line))
                if show_progress:
                    progress_bar(ind, total, prefix='Progress:', suffix='Complete', length=30)
            # VAR consecutive *****************************************
        df = pd.DataFrame(parsed_results)
        df['utc_datetime'] = pd.to_datetime(df['utc_date'].astype(str) + ' ' + df['utc_time'].astype(str))
        return df


def convert_file(inp_file, out_path):
    track = AdrenaTrack(inp_file, out_path)
    print(f' File {track.inp_file_name} read!')
    # track.show_all_fields_index(1)
    print('Parsing data...')
    df = track.trz_parsing(7, False)

    df.to_csv(track.out_file)
    print(df.info())



@benchmark
def main():
    input_directory = r"C:\Users\matar\OneDrive\Documents\ariel2\2023\fastnet_full\NW2"
    out_path = r'C:\Users\matar\OneDrive\Documents\ariel2\KND\2023\2023-07 Fastnet\Logs\NW2'
    input_directory = r"C:\Users\matar\OneDrive\Documents\ariel2\2023\Traces 17 Sep 23\NW2_17 sep"
    input_directory = r"C:\Users\matar\OneDrive\Documents\ariel2\TJV23\problems_logs"
    out_path = r'C:\Users\matar\OneDrive\Documents\ariel2\KND\2023\tjv23'
    # out_path = 'C:/Users/matar/OneDrive/Documents/ariel2/KND'
    for filename in os.listdir(input_directory):
        if filename.endswith('.trc') or filename.endswith('.trz'):
            # Construct the full path of the input file
            inp_file = os.path.join(input_directory, filename)
            convert_file(inp_file, out_path)


if __name__ == "__main__":
    main()
