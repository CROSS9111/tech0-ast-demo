import yfinance as yf
import pandas as pd

# 会社名と証券コード（.T を付与）を定義
companies = {
    "森永製菓": "2201.T",
    "ブルボン": "2208.T",
    "イビデン": "4062.T",
    "日本ピグメントHD": "4119.T",
    "三菱ケミカルG": "4188.T",
    "武田薬品工業": "4502.T",
    "住友ファーマ": "4506.T",
    "エーザイ": "4523.T",
    "ロート製薬": "4527.T",
    "亀田製菓": "2220.T",
    "扶桑化学工業": "4368.T",
    "東和薬品": "4553.T",
    "サワイGHD": "4887.T",
    "大塚HD": "4578.T",
    "小林製薬": "4967.T",
    "NOK株式会社": "7240.T",
    "ニプロ": "8086.T",
    "日新製鋼": "5413.T",
    "第一三共": "4568.T",
}

# 四半期末日を定義
quarter_ends = {
    "2024Q1": "2024-03-29",
    "2024Q2": "2024-06-28",
    "2024Q3": "2024-09-30",
    "2024Q4": "2024-12-30",
    "2025Q1": "2025-03-31",
}

# 結果を格納するデータフレームを作成
# インデックスに会社名と証券コードを表示するための形式に変更
index_with_ticker = [f"{name} ({ticker})" for name, ticker in companies.items()]
df = pd.DataFrame(index=index_with_ticker, columns=quarter_ends.keys())

for name, ticker in companies.items():
    # yfinance でティッカーを取得
    tk = yf.Ticker(ticker)
    # 過去半年分の月次データを取得（十分にカバーするため）
    hist = tk.history(start="2024-01-01", end="2025-04-05", interval="1d")
    # 各四半期末の終値を抜き出し
    index_key = f"{name} ({ticker})"
    for q, d in quarter_ends.items():
        try:
            df.loc[index_key, q] = hist.loc[d]["Close"]
        except KeyError:
            # 取引日の関係で前後にずれる場合は最も近い直前の終値を取得
            try:
                # 指定日以前のデータがあるか確認
                filtered_hist = hist[:d]
                if not filtered_hist.empty:
                    nearest = filtered_hist.iloc[-1]["Close"]
                    df.loc[index_key, q] = nearest
                else:
                    # 過去のデータがない場合は未来の最も近いデータを使用
                    filtered_hist = hist[hist.index > d]
                    if not filtered_hist.empty:
                        nearest = filtered_hist.iloc[0]["Close"]
                        df.loc[index_key, q] = nearest
                    else:
                        df.loc[index_key, q] = float('nan')  # データがない場合はNaNを設定
            except Exception as e:
                print(f"データ取得エラー ({name} ({ticker}), {q}): {e}")
                df.loc[index_key, q] = float('nan')  # エラーが発生した場合もNaNを設定

print(df)