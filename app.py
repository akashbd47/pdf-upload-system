from flask import Flask, request, jsonify
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

# ========= PROCESS STATUS =========

PROCESS_STATUS = {
    "status": "idle",
    "deleted": 0,
    "inserted": 0
}

# ========= FLASK =========

app = Flask(__name__)

# ========= HELPERS (UNCHANGED LOGIC) =========

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

# ========= PARSER (UNCHANGED LOGIC) =========

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

# ========= BACKGROUND PROCESS =========

def background_process(file_path):

    global PROCESS_STATUS

    PROCESS_STATUS["status"] = "processing"

    data = parse_pdf(file_path)

    if not data:
        PROCESS_STATUS["status"] = "failed"
        return

    col = db.collection(COLLECTION)

    deleted = 0

    # ORIGINAL delete logic
    for d in col.stream():
        d.reference.delete()
        deleted += 1

    # ORIGINAL insert logic
    for r in data:
        col.add(r)

    PROCESS_STATUS["status"] = "completed"
    PROCESS_STATUS["deleted"] = deleted
    PROCESS_STATUS["inserted"] = len(data)

# ========= API =========

@app.route("/upload", methods=["POST"])
def upload_api():

    file = request.files['file']

    temp = tempfile.NamedTemporaryFile(delete=False)

    file.save(temp.name)

    threading.Thread(target=background_process, args=(temp.name,)).start()

    return "Upload received. Processing started."


@app.route("/status")
def status():
    return jsonify(PROCESS_STATUS)


@app.route("/")
def home():
    return "Server Running OK"
