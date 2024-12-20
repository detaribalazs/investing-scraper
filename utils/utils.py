import yaml


def load_tickers(file: str = "") -> list:
    if not file:
        file = "./input/tickers.yaml"
    with open(file, 'r') as f:
        tickers = yaml.safe_load(f)
    with open("./input/removed_tickers.yaml", 'r') as f:
        removed_tickers = yaml.safe_load(f)

    final_tickers = []
    removed_tickers = set(list(removed_tickers))
    for ticker in tickers:
        if ticker not in removed_tickers:
            final_tickers.append(ticker)
    return final_tickers
