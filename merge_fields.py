import csv
import yaml

from utils.utils import load_tickers

all_tickers = load_tickers("./input/tickers.yaml")

def merge_files(data: list) -> dict:
    merged_data = data[0]
    for d in data[1:]:
        for k, v in d.items():
            merged_data[k].update(v)
    return merged_data


def main():
    input_files = [
        "./cache/equity_common-2023-12.yaml",
        "./cache/equity_common-2022-12.yaml",
        "./cache/marketcap-2023-12.yaml",
        "./cache/payout_ratio-2023-12.yaml",
        #"./output/ni_cf-2023-12.yaml",
    ]
    output_file = "./output/merged.yaml"
    csv_file = "./output/merged.csv"
    data = []
    for input_file in input_files:
        with open(input_file, "r") as f:
            content = yaml.safe_load(f)
            data.append(content)
    merged_data = merge_files(data)
    with open(output_file, "w") as f:
        yaml.dump(merged_data, f)
    k = list(merged_data.keys())[0]
    header = list(merged_data[k].keys())
    header.insert(0, "Ticker")
    with open(csv_file, "w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for ticker in all_tickers:
            entry = merged_data[ticker]
            entry["Ticker"] = ticker
            row = []
            for h in header:
                cell = str(entry.get(h, "")).strip()
                if h != "Ticker":
                    if cell.endswith("B"):
                        num = float(cell[:-2])
                        num *= 1000000000
                        cell = str(num)
                    if cell.endswith("M"):
                        num = float(cell[:-2])
                        num *= 1000000
                        cell = str(num)
                    if h == "Shares Outstanding" and len(cell) != 0:
                        cell = cell.replace(",", "")
                        num = float(cell)
                        num *= 1000000
                        cell = str(num)
                row.append(cell)
            writer.writerow(row)  #entry.get(h, "") for h in header)


if __name__ == "__main__":
    main()
