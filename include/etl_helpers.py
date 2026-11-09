
import requests

API_URL = "https://api.frankfurter.dev/v2/rates"
SOURCE = "Frankfurter API"


def fetch_rates(base: str, quotes: str) -> list[dict]:
    response = requests.get(API_URL, params={"base": base, "quotes": quotes}, timeout=30)
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"Unexpected or empty API response: {rows!r}")
    return rows


def transform_rates(raw_rows: list[dict], retrieved_at: str) -> list[dict]:
    documents = []
    for row in raw_rows:
        if row["base"] == row["quote"]:  # API adds an identity row (EUR -> EUR = 1)
            continue
        documents.append(
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": round(float(row["rate"]), 6),
                "rate_date": row["date"],
                "source": SOURCE,
                "retrieved_at": retrieved_at,
            }
        )
    return documents


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat()
