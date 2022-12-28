# Finances

This repo contains some scripts, applications and/or programms to analyze financial data.

The data is stored under the folder *data* in csv files, and it was retrieved from <https://www.nasdaq.com>.

- NDX: <https://www.nasdaq.com/market-activity/index/ndx/historical>
- SPX: <https://www.nasdaq.com/market-activity/index/spx/historical>

Older data can be gotten for the NDX from <https://finance.yahoo.com/quote/%5ENDX?p=%5ENDX> and for SPX from <https://www.wsj.com/market-data/quotes/index/SPX/historical-prices>.

To use the historyAnalysis.py script from the command line use:

```bash
./historyAnalysis <index name> <percentage> <function>
```

where

- *index name* is nasdaq or spx
- *percentage* is the decline percentage to be targeted
- *function* is the utility to be used. *decline* by default.
