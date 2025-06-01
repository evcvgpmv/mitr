import random
import sqlite3
from datetime import datetime, timedelta
from collections import defaultdict

# === Setup SQLite Database ===
conn = sqlite3.connect("financial_data.db")
cursor = conn.cursor()

# Create tables
cursor.execute("""
CREATE TABLE IF NOT EXISTS books (
    SAP_BOOK_ID TEXT PRIMARY KEY,
    Bookname TEXT,
    systementity TEXT
)""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS journal_entries (
    DOCUMENT_NUMBER TEXT PRIMARY KEY,
    SAP_BOOK_ID TEXT,
    SAP_BOOK_NAME TEXT,
    COST_CENTER TEXT,
    TRANSACTION_CURRENCY TEXT,
    VALUE REAL,
    ENTRY_DATE TEXT,
    POSTING_DATE TEXT,
    USERNAME TEXT,
    TRANSACTION_TYPE TEXT,
    POSTED_BY TEXT,
    APPROVED_BY TEXT,
    CREATED_TIMESTAMP TEXT,
    UPDATED_TIMESTAMP TEXT,
    SOURCE_SYSTEM TEXT,
    REMARKS TEXT
)""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS balance_records (
    SAP_BOOK_ID TEXT,
    DATE TEXT,
    TOTAL_JOURNALS INTEGER,
    LAST_UPDATED_BY TEXT,
    BALANCE REAL,
    DAILY_CHANGE REAL
)""")

# === Generate Book Records ===
book_records = []
book_name_samples = ["General Ledger", "Accounts Payable", "Accounts Receivable", "Fixed Assets", "Inventory"]
system_entities = ["SAP", "Oracle", "NetSuite", "Dynamics", "QuickBooks"]

for i in range(1, 101):
    book_id = f"B{str(i).zfill(4)}"
    book_name = random.choice(book_name_samples) + f" {random.randint(1, 10)}"
    system_entity = random.choice(system_entities)
    book_records.append((book_id, book_name, system_entity))
    cursor.execute("INSERT INTO books VALUES (?, ?, ?)", (book_id, book_name, system_entity))

# === Generate Journal Entries ===
journal_entries = []
transaction_types = ["Credit", "Debit", "Transfer"]
remarks_samples = ["Monthly accrual", "Correction", "Initial entry", "Reclassification"]

document_counter = 1

for book_id, book_name, system_entity in book_records:
    for _ in range(random.randint(1, 5)):
        entry_date = datetime.now() - timedelta(days=random.randint(0, 30))
        posting_date = entry_date + timedelta(days=random.randint(0, 3))
        value = round(random.uniform(100, 10000), 2)
        doc_num = f"DOC{entry_date.strftime('%Y%m%d')}{str(document_counter).zfill(4)}"
        approved_by = None if random.random() < 0.3 else f"manager_{random.randint(1, 5)}"
        journal_entries.append((
            doc_num, book_id, book_name,
            f"CC{random.randint(1000, 9999)}", "USD", value,
            entry_date.isoformat(), posting_date.isoformat(),
            f"user_{random.randint(1, 50)}",
            random.choice(transaction_types),
            f"user_{random.randint(1, 20)}",
            approved_by,
            datetime.now().isoformat(), datetime.now().isoformat(),
            system_entity, random.choice(remarks_samples)
        ))
        document_counter += 1

cursor.executemany("""
INSERT INTO journal_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", journal_entries)

# === Generate Balance Records ===
balance_data = defaultdict(lambda: {"total_value": 0, "count": 0})

for entry in journal_entries:
    book_id = entry[1]
    balance_data[book_id]["total_value"] += entry[5]
    balance_data[book_id]["count"] += 1

balance_records = []
for book_id in balance_data:
    total = balance_data[book_id]["total_value"]
    count = balance_data[book_id]["count"]
    daily_change = round(total / max(count, 1), 2)
    balance_records.append((
        book_id,
        datetime.now().date().isoformat(),
        count,
        f"user_{random.randint(1, 10)}",
        round(total, 2),
        daily_change
    ))

cursor.executemany("""
INSERT INTO balance_records VALUES (?, ?, ?, ?, ?, ?)
""", balance_records)

# === Finalize ===
conn.commit()
conn.close()

print("Data generation and insertion into SQLite complete.")
