# Quant A-Share Stock Selection

A quantitative stock selection project for the **China A-share market**, built with **Python + AkShare**.

The repository contains multiple **momentum-based stock selection strategies**, focusing on **industry strength and relative price strength (RPS)**.

Data is retrieved automatically using **AkShare**, and the scripts generate Excel reports containing selected industries and stocks.

---

# Strategies Included

## 1️⃣ Industry Trend Selector

Script:

```
a_share_trend_selector.py
```

This strategy uses a **two-stage selection process**:

### Step 1 — Industry Selection

Industries are ranked using momentum indicators:

- **RPS20** – 20-day relative strength
- **RPS50** – 50-day relative strength
- **ΔRPS20** – acceleration of relative strength

Industry score:

```
IndustryScore =
0.5 × RPS20 +
0.3 × RPS50 +
0.2 × ΔRPS20
```

Top industries are selected based on this score.

---

### Step 2 — Stock Selection Within Industry

Stocks inside selected industries are filtered by:

- positive 20 / 60 / 120 day momentum
- RPS ranking
- momentum acceleration
- optional market capitalization filter

Stock score:

```
TrendScore =
0.5 × ret20 +
0.3 × ret60 +
0.2 × ret120
```

Top stocks per industry are selected.

---

## 2️⃣ SNS Momentum Selector

Script:

```
sns_selector.py
```

An alternative stock selection model based on **SNS momentum signals**.

The model ranks stocks according to momentum and relative strength indicators derived from historical price data.

---

# Data Source

Market data is retrieved using **AkShare**:

https://akshare.akfamily.xyz/

Data used includes:

- A-share historical price data
- industry index data
- industry component stocks
- market capitalization

---

# Installation

Clone the repository:

```
git clone https://github.com/Brown9487/quant-stock-selector.git
cd quant-stock-selector
```

Install dependencies:

```
pip install -r requirements.txt
```

---

# Requirements

```
akshare
pandas
tqdm
```

---

# How to Run

Run the industry trend selector:

```
python a_share_trend_selector.py
```

Example with parameters:

```
python a_share_trend_selector.py \
  --start-date 2020-01-01 \
  --end-date 2024-01-01 \
  --industry-top-n 10 \
  --stock-per-industry 5
```

Run the SNS selector:

```
python sns_selector.py
```

---

# Output

The scripts generate Excel reports such as:

```
trend_selector_results_runYYYYMMDD_closeYYYYMMDD.xlsx
```

Excel sheets include:

| Sheet | Description |
|------|-------------|
| industry | selected industries |
| stock | selected stocks |
| diagnostics | intermediate statistics |

---

# Project Structure

```
quant-stock-selector
│
├── a_share_trend_selector.py   # industry trend strategy
├── sns_selector.py             # SNS momentum strategy
├── requirements.txt
└── README.md
```

---

# Notes

This project is intended for **research and educational purposes only**.

It is **not financial advice**.
