# create_dummy_data.py
import sqlite3
import random
from datetime import datetime, timedelta
from decimal import Decimal

DB = "app.db"

# 参考データ（実際の株価データ）
STOCK_DATA = [
    {"name": "森永製菓", "code": "2201", "prices": [2576.79, 2431.25, 2802.92, 2653.47, 2505.0]},
    {"name": "ブルボン", "code": "2208", "prices": [2390.28, 2348.45, 2378.31, 2413.24, 2536.0]},
    {"name": "イビデン", "code": "4062", "prices": [6571.93, 6488.69, 4404.42, 4752.80, 3989.0]},
    {"name": "日本ピグメントHD", "code": "4119", "prices": [2969.22, 3119.38, 3162.97, 2902.38, 3095.0]},
    {"name": "三菱ケミカルG", "code": "4188", "prices": [887.65, 860.30, 900.32, 783.53, 737.0]},
    {"name": "武田薬品工業", "code": "4502", "prices": [3998.72, 3988.21, 4014.94, 4091.26, 4413.0]},
    {"name": "住友ファーマ", "code": "4506", "prices": [404.0, 405.0, 596.0, 563.0, 729.0]},
    {"name": "エーザイ", "code": "4523", "prices": [6008.54, 6378.14, 5247.15, 4249.75, 4145.0]},
    {"name": "ロート製薬", "code": "4527", "prices": [2925.33, 3327.02, 3541.08, 2852.59, 2236.5]},
    {"name": "亀田製菓", "code": "2220", "prices": [4206.42, 4186.69, 4428.48, 4131.60, 3905.0]},
    {"name": "扶桑化学工業", "code": "4368", "prices": [4555.14, 4054.52, 3980.0, 3565.0, 3440.0]},
    {"name": "東和薬品", "code": "4553", "prices": [2847.40, 2910.79, 3010.0, 3355.0, 2679.0]},
    {"name": "サワイGHD", "code": "4887", "prices": [1958.82, 2150.98, 2011.30, 2123.34, 1981.5]},
    {"name": "大塚HD", "code": "4578", "prices": [6169.13, 6722.96, 8026.84, 8600.0, 7753.0]},
    {"name": "小林製薬", "code": "4967", "prices": [4872.35, 5173.08, 5648.49, 6230.0, 5671.0]},
    {"name": "NOK株式会社", "code": "7240", "prices": [1989.67, 2045.11, 2191.47, 2418.04, 2189.5]},
    {"name": "ニプロ", "code": "8086", "prices": [1204.59, 1238.96, 1410.19, 1483.00, 1358.5]},
    {"name": "第一三共", "code": "4568", "prices": [4691.85, 5446.05, 4671.20, 4317.06, 3511.0]}
]

# 四半期の定義
QUARTERS = [
    ("2024", "Q1", "2024-03-31"),
    ("2024", "Q2", "2024-06-30"), 
    ("2024", "Q3", "2024-09-30"),
    ("2024", "Q4", "2024-12-31"),
    ("2025", "Q1", "2025-03-31")
]

def create_dummy_data():
    conn = sqlite3.connect(DB)
    conn.execute("PRAGMA foreign_keys = ON;")
    
    print("🚀 ダミーデータ作成開始...")
    
    # 1. 銘柄マスターデータの挿入
    print("📊 銘柄マスターデータを挿入中...")
    for stock in STOCK_DATA:
        conn.execute("""
            INSERT OR IGNORE INTO securities (security_code, d365_code, security_name) 
            VALUES (?, ?, ?)
        """, (stock["code"], stock["code"], stock["name"]))
    
    # 2. 株価データの挿入
    print("💹 株価データを挿入中...")
    for i, (year, quarter, end_date) in enumerate(QUARTERS):
        for stock in STOCK_DATA:
            if i < len(stock["prices"]):
                price = stock["prices"][i]
                conn.execute("""
                    INSERT OR IGNORE INTO price_quotes (quote_date, security_id, close_price)
                    SELECT ?, security_id, ?
                    FROM securities WHERE security_code = ?
                """, (end_date, price, stock["code"]))
    
    # 3. 売買トランザクションの生成
    print("🔄 売買トランザクションを生成中...")
    
    # 各銘柄に対してランダムな売買履歴を生成
    for stock in STOCK_DATA:
        # 銘柄のsecurity_idを取得
        cursor = conn.execute("SELECT security_id FROM securities WHERE security_code = ?", (stock["code"],))
        security_id = cursor.fetchone()[0]
        
        # 各四半期でランダムな売買を生成
        cumulative_quantity = 0
        
        for i, (year, quarter, end_date) in enumerate(QUARTERS):
            if i < len(stock["prices"]):
                price = stock["prices"][i]
                
                # 四半期内でランダムに1-3回の取引を生成
                num_transactions = random.randint(1, 3)
                
                for _ in range(num_transactions):
                    # 取引タイプを決定（最初は必ず買い）
                    if cumulative_quantity <= 0:
                        txn_type = "BUY"
                    else:
                        txn_type = random.choice(["BUY", "SEL"])
                    
                    # 取引数量（100株単位）
                    if txn_type == "BUY":
                        quantity = random.randint(1, 10) * 100  # 100-1000株
                    else:
                        # 売りの場合は保有数量を超えない
                        max_sell = min(cumulative_quantity, random.randint(1, 5) * 100)
                        quantity = max_sell if max_sell > 0 else 100
                    
                    # 価格にランダムな変動を加える（±5%）
                    price_variation = random.uniform(0.95, 1.05)
                    transaction_price = round(price * price_variation, 2)
                    
                    # 取引日をランダムに設定（四半期内）
                    base_date = datetime.strptime(end_date, "%Y-%m-%d")
                    days_back = random.randint(1, 90)  # 四半期内
                    transaction_date = (base_date - timedelta(days=days_back)).strftime("%Y-%m-%d")
                    
                    # トランザクション挿入
                    conn.execute("""
                        INSERT INTO transactions 
                        (security_id, txn_type, quantity, price, txn_date)
                        VALUES (?, ?, ?, ?, ?)
                    """, (security_id, txn_type, quantity, transaction_price, transaction_date))
                    
                    # 累積数量を更新
                    if txn_type == "BUY":
                        cumulative_quantity += quantity
                    else:
                        cumulative_quantity -= quantity
    
    # 4. 四半期ポジションデータの生成
    print("📈 四半期ポジションデータを生成中...")
    
    for i, (year, quarter, end_date) in enumerate(QUARTERS):
        for stock in STOCK_DATA:
            if i < len(stock["prices"]):
                cursor = conn.execute("SELECT security_id FROM securities WHERE security_code = ?", (stock["code"],))
                security_id = cursor.fetchone()[0]
                
                # 四半期末時点の累積ポジションを計算
                cursor = conn.execute("""
                    SELECT 
                        SUM(CASE WHEN txn_type = 'BUY' THEN quantity ELSE -quantity END) as holding_qty,
                        SUM(CASE WHEN txn_type = 'BUY' THEN quantity * price ELSE -quantity * price END) / 
                        SUM(CASE WHEN txn_type = 'BUY' THEN quantity ELSE -quantity END) as avg_cost
                    FROM transactions 
                    WHERE security_id = ? AND txn_date <= ?
                """, (security_id, end_date))
                
                result = cursor.fetchone()
                holding_qty = result[0] if result[0] else 0
                avg_cost = result[1] if result[1] else 0
                
                if holding_qty > 0:  # 保有がある場合のみ
                    market_price = stock["prices"][i]
                    market_cap = holding_qty * market_price
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO positions_quarter 
                        (security_id, d365_code, security_code, security_name, year, quarter, 
                        holding_qty, avg_cost, market_price, market_cap)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (security_id, stock["code"], stock["code"], stock["name"], 
                        year, quarter, holding_qty, avg_cost, market_price, market_cap))
    
    # 5. 半期ポジションデータの生成
    print("📊 半期ポジションデータを生成中...")
    
    halfyear_periods = [
        ("2024", "H1", "2024-06-30"),  # Q2末
        ("2024", "H2", "2024-12-31"),  # Q4末
        ("2025", "H1", "2025-03-31")   # Q1末（仮）
    ]
    
    for year, half, end_date in halfyear_periods:
        for stock in STOCK_DATA:
            cursor = conn.execute("SELECT security_id FROM securities WHERE security_code = ?", (stock["code"],))
            security_id = cursor.fetchone()[0]
            
            # 半期末時点の累積ポジションを計算
            cursor = conn.execute("""
                SELECT 
                    SUM(CASE WHEN txn_type = 'BUY' THEN quantity ELSE -quantity END) as holding_qty,
                    SUM(CASE WHEN txn_type = 'BUY' THEN quantity * price ELSE -quantity * price END) / 
                    SUM(CASE WHEN txn_type = 'BUY' THEN quantity ELSE -quantity END) as avg_cost
                FROM transactions 
                WHERE security_id = ? AND txn_date <= ?
            """, (security_id, end_date))
            
            result = cursor.fetchone()
            holding_qty = result[0] if result[0] else 0
            avg_cost = result[1] if result[1] else 0
            
            if holding_qty > 0:  # 保有がある場合のみ
                # 半期末の株価を取得
                cursor = conn.execute("""
                    SELECT close_price FROM price_quotes 
                    WHERE security_id = ? AND quote_date <= ? 
                    ORDER BY quote_date DESC LIMIT 1
                """, (security_id, end_date))
                
                price_result = cursor.fetchone()
                if price_result:
                    market_price = price_result[0]
                    market_cap = holding_qty * market_price
                    
                    conn.execute("""
                        INSERT OR REPLACE INTO positions_halfyear 
                        (security_id, d365_code, security_code, security_name, year, half, 
                        holding_qty, avg_cost, market_price, market_cap)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (security_id, stock["code"], stock["code"], stock["name"], 
                        year, half, holding_qty, avg_cost, market_price, market_cap))
    
    # 6. 30%下落判定のダミーデータ
    print("⚠️  30%下落判定データを生成中...")
    
    for year, quarter, _ in QUARTERS:
        for stock in STOCK_DATA:
            # ランダムに30%下落判定を生成
            drop_30pct = random.choice([0, 0, 0, 1])  # 25%の確率で下落
            
            conn.execute("""
                INSERT OR IGNORE INTO drop_judgement 
                (security_code, year, quarter, drop_30pct, judged_at)
                VALUES (?, ?, ?, ?, ?)
            """, (stock["code"], year, quarter, drop_30pct, datetime.now().isoformat()))
    
    conn.commit()
    
    # 結果確認
    print("\n📋 データ作成結果:")
    
    cursor = conn.execute("SELECT COUNT(*) FROM securities")
    print(f"銘柄数: {cursor.fetchone()[0]}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM transactions")
    print(f"取引数: {cursor.fetchone()[0]}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM price_quotes")
    print(f"株価データ数: {cursor.fetchone()[0]}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM positions_quarter")
    print(f"四半期ポジション数: {cursor.fetchone()[0]}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM positions_halfyear")
    print(f"半期ポジション数: {cursor.fetchone()[0]}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM drop_judgement")
    print(f"下落判定データ数: {cursor.fetchone()[0]}")
    
    # サンプルデータの表示
    print("\n🔍 サンプルデータ（v_positions ビュー）:")
    cursor = conn.execute("SELECT * FROM v_positions LIMIT 5")
    for row in cursor.fetchall():
        print(f"  {row[2]} ({row[1]}): 保有数 {row[3]:.0f}株, 平均単価 {row[4]:.2f}円")
    
    conn.close()
    print("\n🎉 ダミーデータ作成完了！")

if __name__ == "__main__":
    create_dummy_data()