from utils.utils import load_tickers

tickers = load_tickers()

def count_duplicates(tickers: list[str]) -> dict[str, int]:
    ret = {}
    for ticker in tickers:
        last = ticker.split(':')[-1]
        if last in ret:
            ret[last] += 1
        else:
            ret[last] = 1
    return ret


if __name__ == '__main__':
    tickers = load_tickers()
    res = count_duplicates(tickers)
    for ticker, count in res.items():
        if count != 1:
            print(f'{ticker}: {count}')
