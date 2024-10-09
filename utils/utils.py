import yaml


def load_tickers(file: str = "") -> list:
    with open(file, 'r') as f:
        tickers = yaml.safe_load(f)
    return tickers
