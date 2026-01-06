import pandas as pd
from .resample import resample_ohlc


def build_yearly(df: pd.DataFrame) -> pd.DataFrame:
    return resample_ohlc(df, "Y")


def build_quarterly(df: pd.DataFrame) -> pd.DataFrame:
    return resample_ohlc(df, "Q")


def build_monthly(df: pd.DataFrame) -> pd.DataFrame:
    return resample_ohlc(df, "M")


def build_weekly(df: pd.DataFrame) -> pd.DataFrame:
    return resample_ohlc(df, "W")
