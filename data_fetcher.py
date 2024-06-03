# -*- encoding: UTF-8 -*-

import akshare as ak
import logging
import talib as tl
import time

import concurrent.futures


def fetch(code_name):
    start_time = time.time()
    stock = code_name[0]
    stock_name = code_name[1]
    data = ak.stock_zh_a_hist(symbol=stock, period="daily", start_date="20240501", adjust="qfq")

    if data is None or data.empty:
        logging.info("股票:" + stock + " name:" + stock_name + " 无数据")
        return

    data['p_change'] = tl.ROC(data['收盘'], 1)
    end_time = time.time()
    return data, (end_time - start_time) * 1000


def run(stocks):
    stocks_data = {}
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_stock = {executor.submit(fetch, stock): stock for stock in stocks}
        for future in concurrent.futures.as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                data, timecost = future.result()
                if data is not None:
                    data = data.astype({'成交量': 'double'})
                    stocks_data[stock] = data
                print(f"Fetching data for {stock} took {timecost} ms")
            except Exception as exc:
                print('%s(%r) generated an exception: %s' % (stock[1], stock[0], exc))
    end_time = time.time()
    logging.info("fetch data time cost: %s ms", (end_time - start_time) * 1000)
    return stocks_data
