import array
import csv
import logging
import math
import sys

import numpy as np
import yaml

from utils.utils import load_tickers
from pandas import DataFrame

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

all_tickers = load_tickers("./input/tickers.yaml")
only_complete = False
input_files = [
    "./output/income_statement:2018_2023.yaml",
    "./output/balance_sheet:2018_2023.yaml",
    "./output/marketcap:2019_2023.yaml"
    #"./output/beta:2023.yaml",
    #"./output/payout_ratio:2023.yaml"
]
excel_file = "./output/panel_full_2019-2023.xlsx"
#excel_file = "/tmp/test.xlsx"
input_header = [
    "Year", "Ticker",
    #"Beta (5 Year)", "Payout Ratio",
    "EBT, Incl. Unusual Items", "Net Income to Stockholders", 'Common Equity', "Market Cap",
    'ROE', "g", "gEBIT", "gNI", "P/B", "lnROE", "lnP/B"]
dates = ["2018", "2019", "2020", "2021", "2022", "2023"]

def merge_files(data: list) -> dict:
    merged_data = data[0]
    for d in data[1:]:
        for k, v in d.items():
            if not merged_data.get(k, False):
                merged_data[k] = {}
            merged_data[k].update(v)
    return merged_data

NA = "=NA()"

class FinancialData:
    data = {}
    tickers = []

    def __init__(self, data: dict, tickers: list[str], requested_years: list[str]):
        self.data = {}
        for ticker in tickers:
            if ticker in data:
                self.data[ticker] = data[ticker]
            else:
                self.data[ticker] = {}
        self.tickers = tickers
        known_dates = self.__get_dates()
        for year in requested_years:
            if year not in known_dates:
                logger.error(f"Requested year {year} is not known")
                sys.exit(1)
        self.dates = requested_years
        self.header = self.__get_headers()

    def append(self, data: dict):
        for ticker in self.tickers:
            if ticker not in data:
                logger.warning(f"No data for ticker: {ticker}")
                continue
            self.data[ticker].update({ticker: data[ticker]})

    def __get_dates(self) -> list[str]:
        dates = set()
        for data_entry in self.data.values():
            for fin_data in data_entry.values():
                dates.update(list(fin_data.keys()))
        l = list(dates)
        l.sort()
        return l

    def __get_headers(self) -> set[str]:
        known_header = {"Ticker", "Year"}
        for data_entry in self.data.values():
            known_header.update(list(data_entry.keys()))
        return known_header

    def query(self, ticker: str, financial: str, date: str):
        try:
            entry = self.data[ticker][financial][date]
            if entry in {"-", "NA", NA, ""}:
                return NA
            try:
                return float(entry)
            except ValueError:
                return NA
        except KeyError:
            return NA

    def write_to_excel(self, filename: str, header: list[str]):
        table = []

        clean_header = []
        column_indices = {}
        calculated_columns = ["ROE",
                              #"g",
        "gEBIT", "gNI", "P/B", "lnROE", "lngEBIT", "lnP/B"]
        dependencies = {
            "ROE": ["Net Income to Stockholders", "Common Equity"],
            "g": ["ROE", "PR"],
            "gNI": ["Net Income to Stockholders"],
            "gEBIT": ["EBT, Incl. Unusual Items"],
            "P/B": ["Common Equity", "Market Cap"],

        }
        for fin_data in header:
            if fin_data in self.header:
                if fin_data not in clean_header:
                    column_indices[fin_data] = len(clean_header)
                    clean_header.append(fin_data)
            if fin_data in calculated_columns:
                # for dep in dependencies.get(fin_data, []):
                #     if dep not in clean_header:
                #         column_indices[dep] = len(clean_header)
                #         clean_header.append(dep)
                column_indices[fin_data] = len(clean_header)
                clean_header.append(fin_data)

        # for fin_data, idx in zip(clean_header, range(len(clean_header))):
        #     if fin_data not in calculated_columns:
        #         for dep in dependencies.get(fin_data, []):
        #             if dep not in clean_header:
        #                 clean_header.insert(idx, dep)

        for ticker in self.tickers:
            #rows = [([0] * len(self.header)) * len(self.dates)]
            rows = [[0 for _ in range(len(clean_header))] for _ in range(len(self.dates)-1)]
            for fin_data in clean_header:
                for date, date_idx in zip(self.dates[1:], range(1, len(self.dates))):
                    if fin_data == "Ticker":
                        rows[date_idx - 1][column_indices[fin_data]] = ticker
                    elif fin_data == "Year":
                        rows[date_idx - 1][column_indices[fin_data]] = int(date)
                    elif fin_data == "ROE":
                        ni_str = self.query(ticker, "Net Income to Stockholders", date)
                        if ni_str == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        try:
                            ni = float(ni_str)
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        prev_date = self.dates[date_idx-1]
                        ce_prev = self.query(ticker, "Common Equity", prev_date)
                        if ce_prev == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        ce = self.query(ticker, "Common Equity", date)
                        if ce == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        try:
                            avg_ce = (float(ce_prev) + float(ce)) / 2.0
                        except TypeError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        roe = ni / avg_ce
                        # if roe < 0:
                        #     roe = 0.001
                        rows[date_idx - 1][column_indices[fin_data]] = roe

                    elif fin_data in {"gEBIT", "gNI"}:
                        metric_map = {
                            "gNI": "Net Income to Stockholders",
                            "gEBIT": "EBT, Incl. Unusual Items",
                        }
                        metric = metric_map[fin_data]
                        prev_date = self.dates[date_idx - 1]
                        ebt_prev_str = self.query(ticker, metric, prev_date)
                        if ebt_prev_str == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        try:
                            ebt_prev = float(ebt_prev_str)
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        ebt_str = self.query(ticker, metric, date)
                        if ebt_str == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        try:
                            ebt = float(ebt_str)
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue

                        g = (ebt - ebt_prev)/ebt_prev
                        # if g < 0:
                        #     g = 0.0001
                        rows[date_idx - 1][column_indices[fin_data]] = g

                    elif fin_data == "g":
                        rows[date_idx - 1][column_indices["g"]] = self.calculate_g(rows, column_indices, date_idx)

                    elif fin_data == "P/B":
                        P = rows[date_idx - 1][column_indices["Market Cap"]]
                        B = rows[date_idx - 1][column_indices["Common Equity"]]
                        if P == NA or B == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                        try:
                            rows[date_idx - 1][column_indices[fin_data]] = float(P)/float(B)
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA

                    elif fin_data in {"lnROE", "lngEBIT", "lnP/B"}:
                        original_metric = fin_data[2:]
                        try:
                            original_value = rows[date_idx - 1][column_indices[original_metric]]
                            if original_value == NA:
                                rows[date_idx - 1][column_indices[fin_data]] = NA
                            rows[date_idx - 1][column_indices[fin_data]] = math.log(float(original_value))
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA

                    else:
                        rows[date_idx - 1][column_indices[fin_data]] = self.query(ticker, fin_data, date)

            if only_complete:
                b = False
                for row in rows:
                    if b:
                        break
                    for cell in row:
                        if b:
                            break
                        if cell == NA:
                            rows = []
                            b = True
                            break
            for row in rows:
                table.append(row)

        df = DataFrame(table, columns=clean_header)
        df.to_excel(filename)

    def calculate_g(self,  rows: list[list], column_indices: dict[str, int], date_idx: int) -> float:
        roe = rows[date_idx - 1][column_indices["ROE"]]
        pr = rows[date_idx - 1][column_indices["Payout Ratio"]]
        if roe == NA or pr == NA:
            return NA
        try:
            return float(roe) * (1 - float(pr))
        except ValueError:
            return NA



def main():
    data = []
    for input_file in input_files:
        with open(input_file, "r") as f:
            content = yaml.safe_load(f)
            data.append(content)
    merged_data = merge_files(data)
    fd = FinancialData(merged_data, tickers=all_tickers, requested_years=dates)
    fd.write_to_excel(excel_file,input_header)


if __name__ == "__main__":
    main()
