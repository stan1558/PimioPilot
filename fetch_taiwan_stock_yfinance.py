import yfinance as yf
import pandas as pd

# 設定股票代碼（台積電）
ticker_symbol = "2330.TW"

# 建立 Ticker 物件
tsmc = yf.Ticker(ticker_symbol)

# 下載近 6 個月的日K資料
daily_data = tsmc.history(period="6mo", interval="1d")
print("📈 台積電近 6 個月日K：")
print(daily_data.head())

# 下載今天的 5 分鐘分K
intraday_data = tsmc.history(period="1d", interval="5m")
print("\n⏱ 台積電今日 5 分鐘分K：")
print(intraday_data.head())

# 存成 CSV
daily_data.to_csv("tsmc_daily.csv", encoding="utf-8-sig")
intraday_data.to_csv("tsmc_intraday.csv", encoding="utf-8-sig")

print("\n✅ 已將資料存成 tsmc_daily.csv 和 tsmc_intraday.csv")
