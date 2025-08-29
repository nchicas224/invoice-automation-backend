from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime

def norm_vendor(s: str) -> str:
    return " ".join(s.split()).upper()

def norm_amount(a: str | float | Decimal) -> str:
     return str(Decimal(str(a)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def norm_date(date: str) -> str:
    return datetime.strptime(date, "%m/%d/%Y").date().isoformat()