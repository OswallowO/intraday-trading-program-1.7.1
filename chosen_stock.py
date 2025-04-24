code = 3526

chosen_stock = {
    'symbol': "2330",        # 股票代號（字串，無前綴）
    'rise': 5.6,             # 漲幅百分比（float）
    'row': {                 # K 線資料（字典），例如：
        'open': 123.4,
        'high': 125.0,
        'low': 122.8,
        'close': 124.5,
        'volume': 1000,
        '昨日收盤價': 120.0,
        '漲停價': 132.0,
        'rise': 3.75,
        'highest': 125.0,
        '5min_pct_increase': 3.5,
        # 可能還有其它欄位…
    }
}

print(chosen_stock['symbol'])
print(type(chosen_stock['symbol']))

print(code)
print(type(code))