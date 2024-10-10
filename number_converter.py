import yaml

cache_file = "./cache/marketcap-2023-12.yaml"
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

    with open(cache_file, "w") as f:
        yaml.dump(yaml_data, f)

