from include.etl_helpers import transform_rates

SAMPLE = [
    {"date": "2026-11-04", "base": "EUR", "quote": "EUR", "rate": 1},
    {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15},
    {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 177.2},
]

def test_shape_and_identity_dropped():
    docs = transform_rates(SAMPLE, "2026-11-05T07:15:00")
    assert len(docs) == 2
    assert docs[0] == {
        "base_currency": "EUR", "target_currency": "USD", "exchange_rate": 1.15,
        "rate_date": "2026-11-04", "source": "Frankfurter API",
        "retrieved_at": "2026-11-05T07:15:00",
    }
