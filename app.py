from flask import Flask, request
import pdfplumber
import firebase_admin
from firebase_admin import credentials, firestore
import re
import os
import json
import threading
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

# ========= HELPERS =========

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


def extract_name(text, loan_sl):

    if not loan_sl:
        return None

    right = text.split(loan_sl, 1)[1]

    right = re.sub(r"(01\d{9}|8801\d{9})", "", right)
    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)
    right = re.sub(r"\d{5,}", "", right)

    words = [w.capitalize() for w in right.split() if w.isalpha()]

    return " ".join(words).strip()


def is_header_or_footer(line):

    keywords = ["bank","statement","report","branch","page","loan case","loan sl","customer name","balance","total"]

    low = line.lower()

    return any(k in low for k in keywords)

# ========= PARSER =========

def parse_pdf(file_path):

    records = []

    with pdfplumber.open(file_path) as pdf:

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

                records.append({
                    "loanSlNo": loan_sl,
                    "customerName": name,
                    "phoneLast11": extract_phone(line),
                    "loanStartDate": extract_date(line),
                    "balance": balance
                })

    return records

# ========= BACKGROUND JOB =========

def background_process(file_path):

    print("Background processing started")

    data = parse_pdf(file_path)

    if not data:
        print("No data parsed")
        return

    col = db.collection(COLLECTION)

    # batch delete
    batch = db.batch()
    count = 0

    for d in col.stream():
        batch.delete(d.reference)
        count += 1

        if count == 400:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    # batch insert
    batch = db.batch()
    count = 0

    for r in data:

        ref = col.document()

        batch.set(ref, r)

        count += 1

        if count == 400:
            batch.commit()
            batch = db.batch()
            count = 0

    if count > 0:
        batch.commit()

    print("Background processing complete")

# ========= API =========

@app.route("/upload", methods=["POST"])
def upload_api():

    file = request.files['file']

    temp = tempfile.NamedTemporaryFile(delete=False)

    file.save(temp.name)

    threading.Thread(target=background_process, args=(temp.name,)).start()

    return "Upload received. Processing started in background."

@app.route("/")
def home():
    return "Server Running OK"
