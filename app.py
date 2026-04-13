from flask import Flask, request
import pdfplumber
import firebase_admin
from firebase_admin import credentials, firestore
import re
import os
import json
import tempfile
import threading


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

        clean = re.sub(r"[^A-Za-z\.]", "", w)

        if clean:

            if clean.isupper():
                words.append(clean)
            else:
                words.append(clean.capitalize())

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
                    "loanDurationMonth": extract_loan_duration(line, loan_sl),
                    "balance": balance
                }

                records.append(record)

    return records
# ========= added it =========
def process_file(path):
    print("Started processing")

    data = parse_pdf(path)

    print("Parsed:", len(data))

    if not data:
        print("No data found")
        return

    deleted, inserted = upload(data)

    print("Done:", deleted, inserted)

# ========= FAST UPLOAD =========

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

        if count == 200:
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

    # ===== SAVE LAST UPDATE TIME =====

    db.collection("MetaData").document("Loan Data Update").set({
        "updateTime": firestore.SERVER_TIMESTAMP
    })

    return deleted, inserted

# ========= API =========

@app.route("/upload", methods=["POST"])
def upload_api():

    file = request.files['file']

    temp = tempfile.NamedTemporaryFile(delete=False)

    file.save(temp.name)

    threading.Thread(target=process_file, args=(temp.name,), daemon=True).start()

    return "Processing started... wait 30 sec"
# ========= PANEL =========

@app.route("/")
def home():
    return """
<!DOCTYPE html>
<html>
<head>

<meta name="viewport" content="width=device-width, initial-scale=1">

<title>Loan Upload Panel</title>

<style>

body{
font-family:Arial;
background:#f4f6fb;
margin:0;
}

.container{
max-width:400px;
margin:60px auto;
background:white;
padding:25px;
border-radius:12px;
box-shadow:0 0 15px rgba(0,0,0,0.08);
}

h2{
text-align:center;
color:#4b3fe4;
}

input{
width:100%;
margin-top:15px;
}

button{
width:100%;
margin-top:15px;
padding:12px;
border:none;
border-radius:8px;
background:#4b3fe4;
color:white;
font-size:16px;
cursor:pointer;
}

button:hover{
background:#372fd1;
}

.status{
margin-top:20px;
padding:12px;
border-radius:8px;
text-align:center;
font-weight:bold;
}

.processing{
background:#fff3cd;
color:#856404;
}

.success{
background:#d4edda;
color:#155724;
}

</style>

</head>

<body>

<div class="container">

<h2>Loan Monthly Upload</h2>

<input type="file" id="file" accept="application/pdf">

<button onclick="upload()">Upload PDF</button>

<div id="result"></div>

</div>

<script>

async function upload(){

let file=document.getElementById("file").files[0];

if(!file){
alert("Select PDF first");
return;
}

let fd=new FormData();
fd.append("file",file);

let result=document.getElementById("result");

result.className="status processing";
result.innerHTML="Processing... please wait";

let res=await fetch("/upload",{method:"POST",body:fd});

let text=await res.text();

result.className="status success";
result.innerHTML=text;

}

</script>

</body>
</html>
"""
