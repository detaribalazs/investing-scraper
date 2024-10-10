import yaml


def load_tickers(file: str = "") -> list:
    with open(file, 'r') as f:
        tickers = yaml.safe_load(f)
    with open("./input/removed_tickers.yaml", 'r') as f:
        removed_tickers = yaml.safe_load(f)

    removed_tickers = set(list(removed_tickers))
    for ticker in tickers:
        if ticker in removed_tickers:
            tickers.remove(ticker)
    return tickers
