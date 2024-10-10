import yaml

cache_file = "./cache/payout_ratio-2023-12.yaml"
if __name__ == "__main__":
    with open(cache_file, "r") as f:
        yaml_data = dict(yaml.safe_load(f))

    if cache_file == "./cache/marketcap-2023-12.yaml":
        for key, item in yaml_data.items():
            if len(item) == 0:
                print(f"Skipping {key}")
                continue
            mc = float(item["Market Cap"])
            yaml_data[key]["Market Cap"] = int(mc)
            shares_str = str(item["Shares Outstanding"]).replace(",", "")
            shares = float(shares_str)
            shares *= 1000000
            yaml_data[key]["Shares Outstanding"] = int(shares)

    if "equity_common" in cache_file:
        for key, item in yaml_data.items():
            if len(item) == 0:
                print(f"Skipping {key}")
                continue
            if item["Common Equity"] in ['', "NA"] :
                continue
            ce = int(float(item["Common Equity"]))
            yaml_data[key]["Common Equity"] = ce

    if "payout_ratio" in cache_file:
        for key, item in yaml_data.items():
            if len(item) == 0:
                print(f"Skipping {key}")
                continue
            if item["Payout Ratio"] in ['', "NA"] :
                continue
            ce_str = item["Payout Ratio"]
            if ce_str.endswith("%"):
                ce_str = ce_str[:-1]
                ce = float(ce_str)/100
                yaml_data[key]["Payout Ratio"] = ce



    with open(cache_file, "w") as f:
        yaml.dump(yaml_data, f)

