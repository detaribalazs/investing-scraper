import yaml

old_cache = "./cache/bak/payout_ratio-2023-12.yaml"
new_cache = "./cache/payout_ratio:2023.yaml"

if __name__ == "__main__":
    with open(old_cache, "r") as f:
        old_data = yaml.safe_load(f)

    new_data = {}
    for ticker, v in old_data.items():
        new_data[ticker] = {
            "Payout Ratio": {
                "2023": v["Payout Ratio"] if v.get("Payout Ratio") else "NA",
            }
        }
    with open(new_cache, "w") as f:
        f.write(yaml.dump(new_data, indent=2))