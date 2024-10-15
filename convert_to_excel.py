import array
import csv
import logging

import numpy as np
import yaml

from utils.utils import load_tickers
from pandas import DataFrame

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

all_tickers = load_tickers("./input/tickers.yaml")
only_complete = True

def merge_files(data: list) -> dict:
    merged_data = data[0]
    for d in data[1:]:
        for k, v in d.items():
            merged_data[k].update(v)
    return merged_data

NA = "=NA()"

class FinancialData:
    data = {}
    tickers = []

    def __init__(self, data: dict, tickers: list[str]):
        self.data = {}
        for ticker in tickers:
            if ticker in data:
                self.data[ticker] = data[ticker]
            else:
                self.data[ticker] = {}
        self.tickers = tickers
        self.dates = self.__get_dates()
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
            if entry == "-":
                return NA
            if entry == "NA":
                return NA
            return entry
        except KeyError:
            return NA

    def write_to_excel(self, filename: str, header: list[str]):
        table = []

        clean_header = []
        column_indices = {}
        calculated_columns = ["ROE", "g"]
        for fin_data in header:
            if fin_data in self.header:
                clean_header.append(fin_data)
                column_indices[fin_data] = len(clean_header) - 1
            if fin_data in calculated_columns:
                clean_header.append(fin_data)
                column_indices[fin_data] = len(clean_header) - 1

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

                    elif fin_data == "g":
                        prev_date = self.dates[date_idx - 1]
                        ebt_prev_str = self.query(ticker, "EBT, Incl. Unusual Items", prev_date)
                        if ebt_prev_str == NA:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        try:
                            ebt_prev = float(ebt_prev_str)
                        except ValueError:
                            rows[date_idx - 1][column_indices[fin_data]] = NA
                            continue
                        ebt_str = self.query(ticker, "EBT, Incl. Unusual Items", date)
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
                    else:
                        if self.data[ticker].get(fin_data, False):
                            if self.data[ticker][fin_data].get(date, False):
                                try:
                                    rows[date_idx - 1][column_indices[fin_data]] = float(self.query(ticker, fin_data, date))
                                except ValueError:
                                    rows[date_idx - 1][column_indices[fin_data]] = NA
                            else:
                                rows[date_idx - 1][column_indices[fin_data]] = NA
                        else:
                            rows[date_idx - 1] = [NA for _ in range(len(clean_header))]

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


def main():
    input_files = [
        "./output/income_statement:2018_2023.yaml",
        "./output/balance_sheet:2018_2023.yaml",
        "./output/marketcap:2019_2023.yaml"
    ]
    #output_file = "./output/merged-beta.yaml"
    excel_file = "./output/panel_data-2.xlsx"
    data = []
    for input_file in input_files:
        with open(input_file, "r") as f:
            content = yaml.safe_load(f)
            data.append(content)
    merged_data = merge_files(data)
    fd = FinancialData(merged_data, tickers=all_tickers)
    fd.write_to_excel(excel_file,
                      ["Year", "Ticker", "EBT, Incl. Unusual Items", "Net Income to Stockholders", 'Common Equity', "Market Cap", 'ROE', "g"])


if __name__ == "__main__":
    main()
