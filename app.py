import pdfplumber
import firebase_admin
from firebase_admin import credentials, firestore
import re

# ========= CONFIG =========
PDF_PATH = "input.pdf"
SERVICE_KEY = "firebase-admin-key.json"
COLLECTION = "LoanMonthlyData"
# =========================


# ========= FIREBASE INIT =========
cred = credentials.Certificate(SERVICE_KEY)
firebase_admin.initialize_app(cred)
db = firestore.client()
# =================================


# ========= HELPERS =========

def extract_phone(text):
    """
    শুধুমাত্র valid BD phone নেবে
    """
    m = re.search(r"(01\d{9}|8801\d{9})", text)
    if not m:
        return None
    digits = re.sub(r"\D", "", m.group())
    return digits[-11:]


def extract_balance(text):
    """
    লাইনের ভেতর থেকে শেষ বড় numeric value (Balance → Total)
    """
    nums = re.findall(r"\d{5,}", text.replace(",", ""))
    return int(nums[-1]) if nums else None


def extract_date(text):
    """
    Loan Start Date
    """
    m = re.search(r"\d{2}[/-]\d{2}[/-]\d{4}", text)
    return m.group() if m else None


def extract_loan_sl(text):
    """
    Loan Sl No pattern
    """
    m = re.search(r"\d{4}-\d{4}-\d{5}", text)
    return m.group() if m else None


def extract_loan_case(text, loan_sl):
    """
    Loan Case No = Loan Sl No এর আগের অংশ
    """
    if not loan_sl:
        return None
    left = text.split(loan_sl)[0].strip()
    parts = left.split()
    return parts[-1] if parts else None


def extract_name(text, loan_sl):
    """
    Customer Name clean করে বের করবে
    """
    if not loan_sl:
        return None

    right = text.split(loan_sl, 1)[1]

    # Remove phone, date, balance
    right = re.sub(r"(01\d{9}|8801\d{9})", "", right)
    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)
    right = re.sub(r"\d{5,}", "", right)

    # Remove UC / U.C / UC:
    right = re.sub(r"\bU\.?C\b[:\-]?", "", right, flags=re.IGNORECASE)

    words = []

    for w in right.split():
        if w.isalpha():
            words.append(w.capitalize())

    name = " ".join(words).strip()
    return name if name else None


# 🔥 NEW FUNCTION (ONLY ADDITION)
def extract_loan_duration(text, loan_sl):

    if not loan_sl:
        return None

    right = text.split(loan_sl, 1)[1]

    # remove date (avoid 17 from 17/02/2026)
    right = re.sub(r"\d{2}[/-]\d{2}[/-]\d{4}", "", right)

    # remove phone
    right = re.sub(r"(01\d{9}|8801\d{9})", "", right)

    # remove big numbers (balance)
    right = re.sub(r"\d{5,}", "", right)

    # only 2 digit numbers
    numbers = re.findall(r"\b\d{2}\b", right)

    for n in numbers:
        val = int(n)

        # realistic loan duration range
        if 6 <= val <= 120:
            return val

    return None


def is_header_or_footer(line):
    """
    Header / footer / title skip
    """
    keywords = [
        "bank", "statement", "report", "branch",
        "page", "loan case", "loan sl",
        "customer name", "balance", "total"
    ]
    low = line.lower()
    return any(k in low for k in keywords)


# ========= PARSER =========

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
                    "loanDurationMonth": extract_loan_duration(line, loan_sl),  # 🔥 ADDED FIELD
                    "balance": balance
                }

                records.append(record)

    return records


# ========= UPLOAD =========

def upload(records):

    col = db.collection(COLLECTION)

    deleted = 0

    for d in col.stream():
        d.reference.delete()
        deleted += 1

    for r in records:
        col.add(r)

    print("\nMonthly Update Complete\n")
    print(f"Old records deleted: {deleted}")
    print(f"New records inserted: {len(records)}")


# ========= MAIN =========

if __name__ == "__main__":

    print("\nMonthly Update Started\n")

    data = parse_pdf(PDF_PATH)

    if not data:
        print("No data parsed. PDF format may differ.")
    else:
        upload(data)

    input("\nPress Enter to exit...")
