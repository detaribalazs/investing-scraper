import yaml
import csv
from utils.utils import load_tickers

csv_path = "/Users/detaribalazs/tmp/ni_export.csv"
output_path = "./cache/ni_cf-2023-12-modified.yaml"
existing_cache_path = "./cache/ni_cf-2023-12.yaml"

if __name__ == "__main__":
    with open(existing_cache_path, "r") as f:
        existing_cache = dict(yaml.safe_load(f))

    known_tickers = list(existing_cache.keys())
    all_tickers = load_tickers("./input/tickers.yaml")

    out_data = existing_cache
    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        csv_list = list(reader)

    header = csv_list[0]
    ni_idx = header.index("Net Income (FC)")
    ticker_idx = header.index("Ticker v2")
    for row in csv_list[1:]:
        ticker = row[ticker_idx]
        if ticker == "BUL:BACB":
            pass
        if row[ticker_idx] in out_data:
            if len(out_data[ticker]) == 0:
                try:
                   ni = int(row[ni_idx])
                except ValueError:
                    ni = row[ni_idx]
                finally:
                    out_data[ticker] = {
                        "Fiscal Year": '2023-12-31',
                        "Net Income (CF)": ni,
                    }
            ni_str = out_data[ticker]["Net Income (CF)"]
            if ni_str not in ['', "NA"] and not ni_str.endswith("T"):
                try:
                    ni = int(float(ni_str.replace(' ', '')))
                    out_data[ticker]["Net Income (CF)"] = ni
                except ValueError:
                    continue
            else:
                try:
                   ni = int(row[ni_idx])
                except ValueError:
                    ni = row[ni_idx]
                finally:
                    out_data[ticker] = {
                        "Fiscal Year": '2023-12-31',
                        "Net Income (CF)": ni,
                    }
        try:
            ni = int(row[ni_idx])
        except ValueError:
            ni = row[ni_idx]
        finally:
            out_data[ticker] = {
                "Fiscal Year": '2023-12-31',
                "Net Income (CF)": ni,
            }
    with open(output_path, "w") as f:
        yaml.dump(out_data, f, indent=2)