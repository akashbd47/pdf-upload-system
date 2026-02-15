from flask import Flask, request, jsonify
import pdfplumber
import firebase_admin
from firebase_admin import credentials, firestore
import re
import os
import json
import tempfile

# ========= CONFIG =========

COLLECTION = "LoanMonthlyData"

# ========= FIREBASE INIT =========

firebase_json = os.environ.get("FIREBASE_KEY")

cred_dict = json.loads(firebase_json)

cred = credentials.Certificate(cred_dict)

firebase_admin.initialize_app(cred)

db = firestore.client()

# ========= FLASK =========

app = Flask(__name__)

# ========= HELPERS (UNCHANGED) =========

def extract_phone(text):
    m = re.search(r"(01\d{9}|8801\d{9})", text)
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group())
    return digits[-11:]


def extract_balance(text):
    nums = re.findall(r"\d{5,}", text.replace(",", ""))
    return int(nums[-1]) if nums else None


def extract_date(text):
    m = re.search(r"\d{2}[/-]\d{2}[/-]\d{4}", text)
    return m.group() if m else None


def extract_loan_sl(text):
    m = re.search(r"\d{4}-\d{4}-\d{5}", text)
    return m.group() if m else None


def extract_loan_case(text, loan_sl):
    if not loan_sl:
        return None
    left = text.split(loan_sl)[0].strip()
    parts = left.split()
    return parts[-1] if parts else None


def extract_name(text, loan_sl):

    if not loan_sl:
        return None

    right = text.split(loan_sl, 1)[1]

    right = re.sub(r"(01\d{9}|8801\d{9})", "", right)
    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)
    right = re.sub(r"\d{5,}", "", right)
    right = re.sub(r"\bU\.?C\b[:\-]?", "", right, flags=re.IGNORECASE)

    words = []

    for w in right.split():
        if w.isalpha():
            words.append(w.capitalize())

    name = " ".join(words).strip()
    return name if name else None


def extract_loan_duration(text, loan_sl):

    if not loan_sl:
        return None

    right = text.split(loan_sl, 1)[1]

    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)
    right = re.sub(r"(01\d{9}|8801\d{9})", "", right)
    right = re.sub(r"\d{5,}", "", right)

    numbers = re.findall(r"\b\d{2}\b", right)

    for n in numbers:
        val = int(n)
        if 6 <= val <= 120:
            return val

    return None


def is_header_or_footer(line):

    keywords = [
        "bank","statement","report","branch",
        "page","loan case","loan sl",
        "customer name","balance","total"
    ]

    low = line.lower()
    return any(k in low for k in keywords)

# ========= PARSER (UNCHANGED) =========

def parse_pdf(pdf_path):

    records = []

    with pdfplumber.open(pdf_path) as pdf:

        for page in pdf.pages:

            text = page.extract_text()

            if not text:
                continue

            lines = [l.strip() for l in text.split("\n") if l.strip()]

            for line in lines:

                if is_header_or_footer(line):
                    continue

                loan_sl = extract_loan_sl(line)

                if not loan_sl:
                    continue

                balance = extract_balance(line)
                name = extract_name(line, loan_sl)

                if not name or balance is None:
                    continue

                record = {
                    "loanCaseNo": extract_loan_case(line, loan_sl),
                    "loanSlNo": loan_sl,
                    "customerName": name,
                    "phoneLast11": extract_phone(line),
                    "loanStartDate": extract_date(line),
                    "loanDurationMonth": extract_loan_duration(line, loan_sl),
                    "balance": balance
                }

                records.append(record)

    return records

# ========= FAST UPLOAD (BATCH DELETE + INSERT) =========

def upload(records):

    col = db.collection(COLLECTION)

    deleted = 0

    docs = list(col.stream())

    batch = db.batch()
    count = 0

    for d in docs:
        batch.delete(d.reference)
        deleted += 1
        count += 1

        if count == 400:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    inserted = 0
    batch = db.batch()
    count = 0

    for r in records:

        ref = col.document()
        batch.set(ref, r)

        inserted += 1
        count += 1

        if count == 400:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    return deleted, inserted

# ========= API =========

@app.route("/upload", methods=["POST"])
def upload_api():

    file = request.files['file']

    temp = tempfile.NamedTemporaryFile(delete=False)

    file.save(temp.name)

    data = parse_pdf(temp.name)

    if not data:
        return "No data parsed"

    deleted, inserted = upload(data)

    return f"Completed | Deleted: {deleted} | Inserted: {inserted}"


@app.route("/")
def home():
    return "Server Running OK"
