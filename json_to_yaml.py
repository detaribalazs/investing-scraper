import json

import yaml

if __name__ == '__main__':
    input_file = "./input/tickers.json"
    output_file = "./input/tickers.yaml"
    with open(input_file, "r") as f:
        data = json.load(f)
    with open(output_file, "w") as f:
        yaml.dump(data, f, indent=2)