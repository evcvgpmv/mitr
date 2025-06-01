import random
import csv
from datetime import datetime, timedelta
from collections import defaultdict

# === File Names ===
BOOKS_CSV = "books.csv"
JOURNALS_CSV = "journal_entries.csv"
BALANCES_CSV = "balance_records.csv"

# === Step 1: Generate Book Records ===
book_records = []
book_name_samples = ["General Ledger", "Accounts Payable", "Accounts Receivable", "Fixed Assets", "Inventory"]
system_entities = ["SAP", "Oracle", "NetSuite", "Dynamics", "QuickBooks"]

for i in range(1, 101):
    book_id = f"B{str(i).zfill(4)}"
    book_name = random.choice(book_name_samples) + f" {random.randint(1, 10)}"
    system_entity = random.choice(system_entities)
    book_records.append({
        "SAP_BOOK_ID": book_id,
        "Bookname": book_name,
        "systementity": system_entity
    })

# === Step 2: Generate 100,000 Journal Entries across 100 Dates ===
journal_entries = []
transaction_types = ["Credit", "Debit", "Transfer"]
remarks_samples = ["Monthly accrual", "Correction", "Initial entry", "Reclassification"]
document_counter = 1

# 100 dates, starting from today and going back
dates_list = [datetime.now() - timedelta(days=i) for i in range(100)]

for entry_date_dt in dates_list:
    entry_date = entry_date_dt.strftime('%Y%m%d')
    for _ in range(1000):  # 1,000 entries per date
        book = random.choice(book_records)
        posting_date_dt = entry_date_dt + timedelta(days=random.randint(0, 3))
        posting_date = posting_date_dt.strftime('%Y%m%d')
        value = round(random.uniform(100, 10000), 2)
        approved_by = None if random.random() < 0.3 else f"manager_{random.randint(1, 5)}"

        journal_entries.append({
            "DOCUMENT_NUMBER": f"DOC{entry_date}{str(document_counter).zfill(6)}",
            "SAP_BOOK_ID": book["SAP_BOOK_ID"],
            "SAP_BOOK_NAME": book["Bookname"],
            "COST_CENTER": f"CC{random.randint(1000, 9999)}",
            "TRANSACTION_CURRENCY": "USD",
            "VALUE": value,
            "ENTRY_DATE": entry_date,
            "POSTING_DATE": posting_date,
            "USERNAME": f"user_{random.randint(1, 50)}",
            "TRANSACTION_TYPE": random.choice(transaction_types),
            "POSTED_BY": f"user_{random.randint(1, 20)}",
            "APPROVED_BY": approved_by,
            "CREATED_TIMESTAMP": datetime.now().isoformat(),
            "UPDATED_TIMESTAMP": datetime.now().isoformat(),
            "SOURCE_SYSTEM": book["systementity"],
            "REMARKS": random.choice(remarks_samples)
        })
        document_counter += 1

# === Step 3: Generate Balance Records by Book and Date ===
balance_data = defaultdict(lambda: {"total_value": 0, "count": 0})

for entry in journal_entries:
    key = (entry["SAP_BOOK_ID"], entry["ENTRY_DATE"])
    balance_data[key]["total_value"] += entry["VALUE"]
    balance_data[key]["count"] += 1

balance_records = []
for (book_id, entry_date), stats in balance_data.items():
    total = stats["total_value"]
    count = stats["count"]
    daily_change = round(total / max(count, 1), 2)
    balance_records.append({
        "SAP_BOOK_ID": book_id,
        "DATE": entry_date,
        "TOTAL_JOURNALS": count,
        "LAST_UPDATED_BY": f"user_{random.randint(1, 10)}",
        "BALANCE": round(total, 2),
        "DAILY_CHANGE": daily_change
    })

# === Step 4: Export to CSV ===

# Books
with open(BOOKS_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=book_records[0].keys())
    writer.writeheader()
    writer.writerows(book_records)

# Journal Entries
with open(JOURNALS_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=journal_entries[0].keys())
    writer.writeheader()
    writer.writerows(journal_entries)

# Balance Records
with open(BALANCES_CSV, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=balance_records[0].keys())
    writer.writeheader()
    writer.writerows(balance_records)

print("✅ Generated 100,000 journal entries and exported to CSV.")
