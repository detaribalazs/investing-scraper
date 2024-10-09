import json
import sys

if __name__ == "__main__":
    input_file = "./output_mc.json"
    ouput_file = "./output_mc_latest.json"

    with open(input_file, "r") as f:
        data = json.load(f)
    out_data = {}
    for row in data:
        ticker = list(row.keys())[0]
        data = row[ticker]
        headers = ["Date", "Shares Outstanding", "Market Cap"]
        cells = {}
        if not data:
            cells = {headers[i]: "" for i in range(len(data))}
        else:
            cells = {headers[i]: data[i] for i in range(len(data))}
        out_data[ticker] = cells

    with open(ouput_file, "w") as f:
        json.dump(out_data, f, indent=2)
