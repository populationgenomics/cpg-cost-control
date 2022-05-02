from datetime import datetime


def usd2aud(amount: int, date: datetime) -> int:
    return amount * 1.35


def aud2usd(amount: int, date: datetime) -> int:
    return amount / 1.35
