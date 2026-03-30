from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import io
import re
from typing import Optional

from app.schemas.imports import (
    ImportFillPreviewRead,
    ImportIssueRead,
    RakutenAuditRowRead,
    ImportTradeCandidateRead,
    RakutenImportAuditResponse,
    RakutenImportPreviewResponse,
)

_HEADER_ALIASES = {
    "date": ("約定日", "受渡日", "取引日"),
    "symbol": {"銘柄コード", "コード", "銘柄ｺｰﾄﾞ"},
    "name": {"銘柄", "銘柄名", "銘柄名称"},
    "side": ("売買区分", "売買"),
    "qty": {"約定数量", "数量", "株数", "約定株数", "約定数", "数量［株］", "数量[株]"},
    "price": {"約定単価", "単価", "価格", "約定価格", "単価［円］", "単価[円]"},
    "fee": {
        "手数料",
        "手数料等",
        "委託手数料",
        "手数料［円］",
        "手数料[円]",
    },
    "other_fee": {
        "諸費用",
        "諸費用［円］",
        "諸費用[円]",
        "手数料・諸費用",
    },
    "tax_fee": {
        "税金等",
        "税金等［円］",
        "税金等[円]",
    },
    "trade_type": ("取引区分", "取引種別", "取引", "商品", "現物信用", "口座区分"),
    "credit_type": ("信用区分", "新規返済", "建区分", "新規建区分", "信用新規建区分"),
    "market": {"市場", "市場名", "市場名称"},
    "build_date": {"建約定日", "建日付"},
    "build_price": {"建単価［円］", "建単価[円]", "建単価"},
    "build_fee": {"建手数料［円］", "建手数料[円]", "建手数料"},
    "build_fee_tax": {"建手数料消費税［円］", "建手数料消費税[円]", "建手数料消費税"},
}


@dataclass
class _RawCsvTrade:
    line: int
    symbol: str
    name: str
    side: str
    position_side: str
    date: str
    qty: int
    price: Decimal
    fee: int
    fee_commission_jpy: int = 0
    fee_tax_jpy: int = 0
    fee_other_jpy: int = 0
    build_date: Optional[str] = None
    build_price: Optional[Decimal] = None
    build_fee: int = 0
    build_fee_commission_jpy: int = 0
    build_fee_tax_jpy: int = 0
    is_credit_close: bool = False


@dataclass
class _AggregatedTrade:
    symbol: str
    name: str
    side: str
    position_side: str
    date: str
    qty: int
    price: Decimal
    fee: int
    fee_commission_jpy: int
    fee_tax_jpy: int
    fee_other_jpy: int
    lines: list[int]
    row_signatures: list[str]
    build_date: Optional[str] = None
    build_price: Optional[Decimal] = None
    build_fee: int = 0
    build_fee_commission_jpy: int = 0
    build_fee_tax_jpy: int = 0
    is_credit_close: bool = False


@dataclass
class _OpenLot:
    symbol: str
    name: str
    position_side: str
    date: str
    qty: int
    price: Decimal
    remaining_qty: int
    remaining_fee: int
    remaining_fee_commission_jpy: int
    remaining_fee_tax_jpy: int
    remaining_fee_other_jpy: int
    lines: list[int]
    row_signatures: list[str]
    source_position_key: str
    next_sequence: int = 1


def _normalize_header(text: str) -> str:
    return str(text or "").strip().replace("\ufeff", "")


def _map_headers(headers: list[str]) -> dict[str, str]:
    mapped: dict[str, str] = {}
    normalized = {_normalize_header(h): h for h in headers}
    for canonical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                mapped[canonical] = normalized[alias]
                break
    return mapped


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _parse_date(value: object) -> Optional[str]:
    text = _clean_text(value)
    if not text:
        return None
    text = text.replace("年", "-").replace("月", "-").replace("日", "")
    text = text.replace("/", "-").replace(".", "-")
    match = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", text)
    if not match:
        return None
    y, m, d = match.groups()
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def _parse_jp_decimal(value: object) -> Optional[Decimal]:
    text = _clean_text(value)
    if not text:
        return None
    text = (
        text.replace(",", "")
        .replace("円", "")
        .replace("株", "")
        .replace("口", "")
        .replace("￥", "")
        .replace("¥", "")
        .strip()
    )
    if text in {"", "-", "—"}:
        return None
    sign = -1 if text.startswith("-") else 1
    text = text.lstrip("+-")
    if not re.match(r"^\d+(\.\d+)?$", text):
        return None
    return Decimal(sign) * Decimal(text).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _parse_jp_int(value: object) -> Optional[int]:
    dec = _parse_jp_decimal(value)
    if dec is None:
        return None
    return int(dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _price_text(value: Decimal) -> str:
    return format(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f")


def _parse_side(value: object) -> Optional[str]:
    text = _clean_text(value)
    if not text:
        return None
    if "買" in text:
        return "buy"
    if "売" in text:
        return "sell"
    return None


def _row_value(row: dict[str, str], headers: dict[str, str], key: str) -> str:
    return _clean_text(row.get(headers.get(key, ""), ""))


def _trade_context(row: dict[str, str], headers: dict[str, str]) -> tuple[str, str, str]:
    trade_type = _row_value(row, headers, "trade_type")
    credit_type = _row_value(row, headers, "credit_type")
    side_text = _row_value(row, headers, "side")
    return trade_type, credit_type, side_text


def _is_credit_close_row(row: dict[str, str], headers: dict[str, str]) -> bool:
    trade_type, credit_type, side_text = _trade_context(row, headers)
    combined = " ".join(part for part in (trade_type, credit_type, side_text) if part)
    return any(marker in combined for marker in ("信用返済", "返済売", "売埋", "返済買", "買埋"))


def _parse_position_side(row: dict[str, str], headers: dict[str, str]) -> str:
    trade_type, credit_type, side_text = _trade_context(row, headers)
    combined = " ".join(part for part in (trade_type, credit_type, side_text) if part)
    if any(marker in combined for marker in ("売建", "新規売", "返済買", "買埋")):
        return "short"
    return "long"


def _is_supported_domestic_stock(row: dict[str, str], headers: dict[str, str]) -> bool:
    trade_type, credit_type, side_text = _trade_context(row, headers)
    market = _clean_text(row.get(headers.get("market", ""), ""))
    if trade_type:
        if "先物" in trade_type or "オプション" in trade_type or "投信" in trade_type:
            return False
        if "信用" in trade_type:
            combined = " ".join(part for part in (trade_type, credit_type, side_text) if part)
            if any(marker in combined for marker in ("買建", "新規買", "返済売", "売埋", "売建", "新規売", "返済買", "買埋", "新規", "返済")):
                return True
            return bool(side_text and _parse_side(side_text))
        if "現物" in trade_type:
            return True
    if market and ("米" in market or "NASDAQ" in market.upper() or "NYSE" in market.upper()):
        return False
    return True


def _parse_row_side(row: dict[str, str], headers: dict[str, str]) -> Optional[str]:
    trade_type, credit_type, side_text = _trade_context(row, headers)
    side = _parse_side(side_text)
    if side:
        return side

    combined = " ".join(part for part in (credit_type, trade_type) if part)
    if any(marker in combined for marker in ("買建", "新規買", "返済買", "買埋")):
        return "buy"
    if any(marker in combined for marker in ("売建", "新規売", "返済売", "売埋")):
        return "sell"
    return None


def _row_signature(raw: _RawCsvTrade) -> str:
    base = f"{raw.symbol}|{raw.name}|{raw.side}|{raw.date}|{raw.qty}|{raw.price}|{raw.fee}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _aggregate_rows(rows: list[_RawCsvTrade]) -> list[_AggregatedTrade]:
    grouped: dict[tuple[str, str, str, str, str, str, bool], list[_RawCsvTrade]] = {}
    for row in rows:
        key = (
            row.symbol,
            row.side,
            row.position_side,
            row.date,
            row.build_date or "",
            str(row.build_price or ""),
            bool(row.is_credit_close),
        )
        grouped.setdefault(key, []).append(row)

    aggregated: list[_AggregatedTrade] = []
    for (symbol, side, position_side, date, build_date, build_price, is_credit_close), items in grouped.items():
        total_qty = sum(item.qty for item in items)
        total_fee = sum(item.fee for item in items)
        total_fee_commission = sum(item.fee_commission_jpy for item in items)
        total_fee_tax = sum(item.fee_tax_jpy for item in items)
        total_fee_other = sum(item.fee_other_jpy for item in items)
        total_build_fee = sum(item.build_fee for item in items)
        total_build_fee_commission = sum(item.build_fee_commission_jpy for item in items)
        total_build_fee_tax = sum(item.build_fee_tax_jpy for item in items)
        weighted_total = sum(item.price * item.qty for item in items)
        avg_price = (Decimal(weighted_total) / Decimal(total_qty)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        avg_build_price = None
        if any(item.build_price is not None for item in items):
            build_weighted_total = sum((item.build_price or Decimal("0")) * item.qty for item in items)
            avg_build_price = (Decimal(build_weighted_total) / Decimal(total_qty)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        name = next((item.name for item in items if item.name), symbol)
        lines = sorted(item.line for item in items)
        row_signatures = sorted(_row_signature(item) for item in items)
        aggregated.append(
            _AggregatedTrade(
                symbol=symbol,
                name=name,
                side=side,
                position_side=position_side,
                date=date,
                qty=total_qty,
                price=avg_price,
                fee=total_fee,
                fee_commission_jpy=total_fee_commission,
                fee_tax_jpy=total_fee_tax,
                fee_other_jpy=total_fee_other,
                lines=lines,
                row_signatures=row_signatures,
                build_date=build_date or None,
                build_price=avg_build_price if build_price else None,
                build_fee=total_build_fee,
                build_fee_commission_jpy=total_build_fee_commission,
                build_fee_tax_jpy=total_build_fee_tax,
                is_credit_close=is_credit_close,
            )
        )
    aggregated.sort(key=lambda item: (item.symbol, item.date, 0 if item.side in {"buy", "sell"} and item.is_credit_close is False else 1))
    return aggregated


def _position_key(symbol: str, position_side: str, buy_date: str, buy_price: Decimal) -> str:
    base = "|".join(
        [
            "rakuten",
            "JP",
            position_side,
            symbol,
            buy_date,
            _price_text(buy_price),
        ]
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _candidate_signature(
    buy_symbol: str,
    buy_date: str,
    buy_qty: int,
    buy_price: Decimal,
    buy_fee: int,
    source_position_key: str,
    source_lot_sequence: int,
    buy_row_signatures: list[str],
    *,
    sell: Optional[_AggregatedTrade] = None,
) -> str:
    parts = [
        "rakuten",
        "JP",
        "closed" if sell is not None else "open",
        buy_symbol,
        source_position_key,
        str(source_lot_sequence),
        buy_date,
        str(buy_qty),
        _price_text(buy_price),
        str(buy_fee),
        ",".join(buy_row_signatures),
    ]
    if sell is not None:
        parts.extend(
            [
                sell.date,
                str(buy_qty),
                _price_text(sell.price),
                str(sell.fee),
                ",".join(sell.row_signatures),
            ]
        )
    base = "|".join(parts)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _allocate_fee_portion(total_fee: int, portion_qty: int, total_qty: int) -> int:
    if total_fee <= 0 or portion_qty <= 0 or total_qty <= 0:
        return 0
    if portion_qty >= total_qty:
        return max(0, int(total_fee))
    allocated = (Decimal(total_fee) * Decimal(portion_qty) / Decimal(total_qty)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    )
    return max(0, min(int(total_fee), int(allocated)))


def _allocate_fee_breakdown(
    *,
    fee_total: int,
    fee_commission_jpy: int,
    fee_tax_jpy: int,
    fee_other_jpy: int,
    portion_qty: int,
    total_qty: int,
) -> tuple[int, int, int, int]:
    commission = _allocate_fee_portion(fee_commission_jpy, portion_qty, total_qty)
    tax = _allocate_fee_portion(fee_tax_jpy, portion_qty, total_qty)
    other = _allocate_fee_portion(fee_other_jpy, portion_qty, total_qty)
    total = _allocate_fee_portion(fee_total, portion_qty, total_qty)
    # Prefer explicit total from CSV, but keep components internally consistent when total is short.
    return commission, tax, other, max(total, commission + tax + other)


def _find_matching_open_lot(open_lots: list[_OpenLot], close_trade: _AggregatedTrade) -> Optional[_OpenLot]:
    if close_trade.build_date and close_trade.build_price:
        for lot in open_lots:
            if lot.date == close_trade.build_date and lot.price == close_trade.build_price:
                return lot
    return open_lots[0] if open_lots else None


def _synthetic_open_lot_from_credit_close(close_trade: _AggregatedTrade) -> _OpenLot:
    open_date = close_trade.build_date or close_trade.date
    open_price = Decimal(str(close_trade.build_price or close_trade.price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    source_key = _position_key(close_trade.symbol, close_trade.position_side, open_date, open_price)
    return _OpenLot(
        symbol=close_trade.symbol,
        name=close_trade.name,
        position_side=close_trade.position_side,
        date=open_date,
        qty=close_trade.qty,
        price=open_price,
        remaining_qty=close_trade.qty,
        remaining_fee=max(0, int(close_trade.build_fee or 0)),
        remaining_fee_commission_jpy=max(0, int(close_trade.build_fee_commission_jpy or 0)),
        remaining_fee_tax_jpy=max(0, int(close_trade.build_fee_tax_jpy or 0)),
        remaining_fee_other_jpy=0,
        lines=[],
        row_signatures=[f"synthetic:{source_key}"],
        source_position_key=source_key,
    )


def _candidate_from_buy_sell(
    open_lot: _OpenLot,
    close_trade: _AggregatedTrade,
    matched_qty: int,
    open_fee: tuple[int, int, int, int],
    close_fee: tuple[int, int, int, int],
    *,
    is_partial_exit: bool,
    remaining_qty_after_sell: int,
) -> ImportTradeCandidateRead:
    sequence = open_lot.next_sequence
    open_lot.next_sequence += 1
    close_preview = _AggregatedTrade(
        symbol=close_trade.symbol,
        name=close_trade.name,
        side=close_trade.side,
        position_side=close_trade.position_side,
        date=close_trade.date,
        qty=matched_qty,
        price=close_trade.price,
        fee=close_fee[3],
        fee_commission_jpy=close_fee[0],
        fee_tax_jpy=close_fee[1],
        fee_other_jpy=close_fee[2],
        lines=close_trade.lines,
        row_signatures=close_trade.row_signatures,
    )
    buy_preview = None
    sell_preview = None
    if open_lot.position_side == "short":
        sell_preview = ImportFillPreviewRead(
            date=open_lot.date,
            price=float(open_lot.price),
            qty=matched_qty,
            fee=open_fee[3],
            fee_commission_jpy=open_fee[0],
            fee_tax_jpy=open_fee[1],
            fee_other_jpy=open_fee[2],
            fee_total_jpy=open_fee[3],
        )
        buy_preview = ImportFillPreviewRead(
            date=close_trade.date,
            price=float(close_trade.price),
            qty=matched_qty,
            fee=close_fee[3],
            fee_commission_jpy=close_fee[0],
            fee_tax_jpy=close_fee[1],
            fee_other_jpy=close_fee[2],
            fee_total_jpy=close_fee[3],
        )
    else:
        buy_preview = ImportFillPreviewRead(
            date=open_lot.date,
            price=float(open_lot.price),
            qty=matched_qty,
            fee=open_fee[3],
            fee_commission_jpy=open_fee[0],
            fee_tax_jpy=open_fee[1],
            fee_other_jpy=open_fee[2],
            fee_total_jpy=open_fee[3],
        )
        sell_preview = ImportFillPreviewRead(
            date=close_trade.date,
            price=float(close_trade.price),
            qty=matched_qty,
            fee=close_fee[3],
            fee_commission_jpy=close_fee[0],
            fee_tax_jpy=close_fee[1],
            fee_other_jpy=close_fee[2],
            fee_total_jpy=close_fee[3],
        )
    return ImportTradeCandidateRead(
        source_signature=_candidate_signature(
            open_lot.symbol,
            open_lot.date,
            matched_qty,
            open_lot.price,
            open_fee[3],
            open_lot.source_position_key,
            sequence,
            open_lot.row_signatures,
            sell=close_preview,
        ),
        source_position_key=open_lot.source_position_key,
        source_lot_sequence=sequence,
        symbol=open_lot.symbol,
        name=open_lot.name or close_trade.name or open_lot.symbol,
        market="JP",
        position_side=open_lot.position_side,
        buy=buy_preview,
        sell=sell_preview,
        source_lines=sorted(set([*open_lot.lines, *close_trade.lines])),
        already_imported=False,
        is_partial_exit=is_partial_exit,
        remaining_qty_after_sell=max(0, remaining_qty_after_sell),
    )


def _candidate_from_open_lot(open_lot: _OpenLot, *, is_partial_exit: bool) -> ImportTradeCandidateRead:
    sequence = open_lot.next_sequence
    open_lot.next_sequence += 1
    fill_preview = ImportFillPreviewRead(
        date=open_lot.date,
        price=float(open_lot.price),
        qty=open_lot.remaining_qty,
        fee=open_lot.remaining_fee,
        fee_commission_jpy=open_lot.remaining_fee_commission_jpy,
        fee_tax_jpy=open_lot.remaining_fee_tax_jpy,
        fee_other_jpy=open_lot.remaining_fee_other_jpy,
        fee_total_jpy=open_lot.remaining_fee,
    )
    return ImportTradeCandidateRead(
        source_signature=_candidate_signature(
            open_lot.symbol,
            open_lot.date,
            open_lot.remaining_qty,
            open_lot.price,
            open_lot.remaining_fee,
            open_lot.source_position_key,
            sequence,
            open_lot.row_signatures,
        ),
        source_position_key=open_lot.source_position_key,
        source_lot_sequence=sequence,
        symbol=open_lot.symbol,
        name=open_lot.name or open_lot.symbol,
        market="JP",
        position_side=open_lot.position_side,
        buy=fill_preview if open_lot.position_side == "long" else None,
        sell=fill_preview if open_lot.position_side == "short" else None,
        source_lines=sorted(open_lot.lines),
        already_imported=False,
        is_partial_exit=is_partial_exit,
        remaining_qty_after_sell=open_lot.remaining_qty,
    )


def _pair_round_trips(rows: list[_AggregatedTrade]) -> tuple[list[ImportTradeCandidateRead], list[ImportIssueRead], list[ImportIssueRead]]:
    candidates: list[ImportTradeCandidateRead] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []
    by_symbol: dict[tuple[str, str], list[_AggregatedTrade]] = {}
    for row in rows:
        by_symbol.setdefault((row.symbol, row.position_side), []).append(row)

    for (symbol, position_side), items in by_symbol.items():
        open_side = "buy" if position_side == "long" else "sell"
        open_lots: list[_OpenLot] = []
        for item in sorted(items, key=lambda row: (row.date, 0 if row.side == open_side else 1)):
            if item.side == open_side:
                open_lots.append(
                    _OpenLot(
                        symbol=item.symbol,
                        name=item.name,
                        position_side=position_side,
                        date=item.date,
                        qty=item.qty,
                        price=item.price,
                        remaining_qty=item.qty,
                        remaining_fee=item.fee,
                        remaining_fee_commission_jpy=item.fee_commission_jpy,
                        remaining_fee_tax_jpy=item.fee_tax_jpy,
                        remaining_fee_other_jpy=item.fee_other_jpy,
                        lines=item.lines,
                        row_signatures=item.row_signatures,
                        source_position_key=_position_key(item.symbol, position_side, item.date, item.price),
                    )
                )
                continue

            remaining_close_qty = item.qty
            remaining_close_fee = item.fee
            remaining_close_commission = item.fee_commission_jpy
            remaining_close_tax = item.fee_tax_jpy
            remaining_close_other = item.fee_other_jpy

            while remaining_close_qty > 0:
                open_lot = _find_matching_open_lot(open_lots, item)
                if open_lot is None:
                    if item.is_credit_close and item.build_date and item.build_price:
                        open_lot = _synthetic_open_lot_from_credit_close(item)
                    else:
                        skipped.append(
                            ImportIssueRead(
                                line=item.lines[0] if item.lines else None,
                                code="sell_without_buy",
                                message=f"{symbol} の返済に対応する建玉が見つかりません。",
                            )
                        )
                        break

                lot_qty_before = open_lot.remaining_qty
                close_qty_before = remaining_close_qty
                matched_qty = min(lot_qty_before, close_qty_before)
                open_fee = _allocate_fee_breakdown(
                    fee_total=open_lot.remaining_fee,
                    fee_commission_jpy=open_lot.remaining_fee_commission_jpy,
                    fee_tax_jpy=open_lot.remaining_fee_tax_jpy,
                    fee_other_jpy=open_lot.remaining_fee_other_jpy,
                    portion_qty=matched_qty,
                    total_qty=lot_qty_before,
                )
                close_fee = _allocate_fee_breakdown(
                    fee_total=remaining_close_fee,
                    fee_commission_jpy=remaining_close_commission,
                    fee_tax_jpy=remaining_close_tax,
                    fee_other_jpy=remaining_close_other,
                    portion_qty=matched_qty,
                    total_qty=close_qty_before,
                )
                remaining_after_sell = max(0, lot_qty_before - matched_qty)
                is_partial_exit = open_lot.qty != matched_qty or item.qty != matched_qty

                candidates.append(
                    _candidate_from_buy_sell(
                        open_lot,
                        item,
                        matched_qty,
                        open_fee,
                        close_fee,
                        is_partial_exit=is_partial_exit,
                        remaining_qty_after_sell=remaining_after_sell,
                    )
                )

                open_lot.remaining_qty -= matched_qty
                open_lot.remaining_fee = max(0, open_lot.remaining_fee - open_fee[3])
                open_lot.remaining_fee_commission_jpy = max(0, open_lot.remaining_fee_commission_jpy - open_fee[0])
                open_lot.remaining_fee_tax_jpy = max(0, open_lot.remaining_fee_tax_jpy - open_fee[1])
                open_lot.remaining_fee_other_jpy = max(0, open_lot.remaining_fee_other_jpy - open_fee[2])
                remaining_close_qty -= matched_qty
                remaining_close_fee = max(0, remaining_close_fee - close_fee[3])
                remaining_close_commission = max(0, remaining_close_commission - close_fee[0])
                remaining_close_tax = max(0, remaining_close_tax - close_fee[1])
                remaining_close_other = max(0, remaining_close_other - close_fee[2])

                if open_lot in open_lots and open_lot.remaining_qty <= 0:
                    open_lots.remove(open_lot)

        for open_lot in open_lots:
            candidates.append(_candidate_from_open_lot(open_lot, is_partial_exit=open_lot.remaining_qty != open_lot.qty))

    candidates.sort(
        key=lambda item: (
            (item.buy.date if item.buy is not None else item.sell.date if item.sell is not None else ""),
            item.symbol,
            0 if item.sell is not None else 1,
            item.sell.date if item.sell is not None else "",
            item.source_lot_sequence,
        )
    )
    return candidates, skipped, errors


def parse_rakuten_domestic_csv(content: str, filename: Optional[str] = None) -> RakutenImportPreviewResponse:
    normalized = str(content or "").replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.DictReader(io.StringIO(normalized))
    headers = _map_headers(reader.fieldnames or [])
    required = {"date", "symbol", "name", "side", "qty", "price"}
    missing = sorted(required - set(headers.keys()))
    if missing:
        return RakutenImportPreviewResponse(
            broker="rakuten",
            market_scope="JP",
            filename=filename,
            candidate_count=0,
            skipped_count=0,
            error_count=1,
            candidates=[],
            skipped=[],
            errors=[
                ImportIssueRead(
                    line=None,
                    code="missing_headers",
                    message=f"CSVヘッダーが不足しています: {', '.join(missing)}",
                )
            ],
        )

    parsed_rows: list[_RawCsvTrade] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []

    for index, row in enumerate(reader, start=2):
        if not any(_clean_text(v) for v in row.values()):
            continue
        if not _is_supported_domestic_stock(row, headers):
            skipped.append(
                ImportIssueRead(
                    line=index,
                    code="unsupported_product",
                    message="国内株の現物・信用買い以外の行はMVP対象外のためスキップしました。",
                )
            )
            continue

        date = _parse_date(row.get(headers["date"]))
        symbol = _clean_text(row.get(headers["symbol"]))
        name = _clean_text(row.get(headers["name"]))
        side = _parse_row_side(row, headers)
        position_side = _parse_position_side(row, headers)
        qty = _parse_jp_int(row.get(headers["qty"]))
        price = _parse_jp_decimal(row.get(headers["price"]))
        is_credit_close = _is_credit_close_row(row, headers)
        build_date = _parse_date(row.get(headers.get("build_date", "")))
        build_price = _parse_jp_decimal(row.get(headers.get("build_price", "")))
        build_fee_commission = _parse_jp_int(row.get(headers.get("build_fee", ""))) or 0
        build_fee_tax = _parse_jp_int(row.get(headers.get("build_fee_tax", ""))) or 0
        build_fee = build_fee_commission + build_fee_tax
        fee_commission = _parse_jp_int(row.get(headers.get("fee", ""))) or 0
        fee_other = _parse_jp_int(row.get(headers.get("other_fee", ""))) or 0
        fee_tax = _parse_jp_int(row.get(headers.get("tax_fee", ""))) or 0
        fee = fee_commission + fee_other + fee_tax

        if not all([date, symbol, name, side]) or qty is None or price is None:
            errors.append(
                ImportIssueRead(
                    line=index,
                    code="invalid_row",
                    message="日付・銘柄コード・銘柄名・売買・数量・価格のいずれかを解釈できませんでした。",
                )
            )
            continue
        if qty <= 0 or price <= 0:
            errors.append(
                ImportIssueRead(
                    line=index,
                    code="invalid_numeric",
                    message="数量または価格が0以下のため取り込めません。",
                )
            )
            continue

        parsed_rows.append(
            _RawCsvTrade(
                line=index,
                symbol=symbol,
                name=name,
                side=side,
                position_side=position_side,
                date=date,
                qty=qty,
                price=price,
                fee=max(0, fee),
                fee_commission_jpy=max(0, fee_commission),
                fee_tax_jpy=max(0, fee_tax),
                fee_other_jpy=max(0, fee_other),
                build_date=build_date,
                build_price=build_price,
                build_fee=max(0, build_fee),
                build_fee_commission_jpy=max(0, build_fee_commission),
                build_fee_tax_jpy=max(0, build_fee_tax),
                is_credit_close=is_credit_close,
            )
        )

    aggregated = _aggregate_rows(parsed_rows)
    candidates, pair_skipped, pair_errors = _pair_round_trips(aggregated)
    skipped.extend(pair_skipped)
    errors.extend(pair_errors)

    return RakutenImportPreviewResponse(
        broker="rakuten",
        market_scope="JP",
        filename=filename,
        candidate_count=len(candidates),
        skipped_count=len(skipped),
        error_count=len(errors),
        candidates=candidates,
        skipped=skipped,
        errors=errors,
    )


def _parse_realized_pl_csv(content: str) -> list[RakutenAuditRowRead]:
    normalized = str(content or "").replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.DictReader(io.StringIO(normalized))
    rows: list[RakutenAuditRowRead] = []
    for row in reader:
        symbol = _clean_text(row.get("銘柄コード"))
        if not symbol:
            continue
        sell_date = _parse_date(row.get("約定日"))
        qty = _parse_jp_int(row.get("数量[株]"))
        sell_price = _parse_jp_decimal(row.get("売却/決済単価[円]"))
        buy_price = _parse_jp_decimal(row.get("平均取得価額[円]"))
        realized_profit = _parse_jp_int(row.get("実現損益[円]"))
        trade_label = _clean_text(row.get("取引"))
        if not sell_date or qty is None or sell_price is None or buy_price is None or realized_profit is None:
            continue
        rows.append(
            RakutenAuditRowRead(
                symbol=symbol,
                name=_clean_text(row.get("銘柄名")),
                sell_date=sell_date,
                qty=max(1, qty),
                sell_price=float(sell_price),
                buy_price_or_avg_cost=float(buy_price),
                rakuten_profit_jpy=float(realized_profit),
                reason_code="unsupported_short_previously" if "買埋" in trade_label else None,
            )
        )
    return rows


def _candidate_profit_jpy(item: ImportTradeCandidateRead) -> float:
    if item.sell is None or item.buy is None:
        return 0.0
    profit = (Decimal(str(item.sell.price)) - Decimal(str(item.buy.price))) * Decimal(item.buy.qty)
    profit -= Decimal(item.buy.fee_total_jpy or item.buy.fee or 0)
    profit -= Decimal(item.sell.fee_total_jpy or item.sell.fee or 0)
    return float(profit.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _audit_key(symbol: str, sell_date: str, qty: int, sell_price: float, buy_price: float) -> tuple[str, str, int, str, str]:
    return (
        symbol,
        sell_date,
        int(qty),
        _price_text(Decimal(str(sell_price))),
        _price_text(Decimal(str(buy_price))),
    )


def _audit_key_loose(symbol: str, sell_date: str, qty: int, sell_price: float) -> tuple[str, str, int, str]:
    return (
        symbol,
        sell_date,
        int(qty),
        _price_text(Decimal(str(sell_price))),
    )


def audit_rakuten_tradehistory_against_realized(
    tradehistory_content: str,
    *,
    tradehistory_filename: Optional[str] = None,
    realized_content: str,
) -> RakutenImportAuditResponse:
    preview = parse_rakuten_domestic_csv(tradehistory_content, tradehistory_filename)
    tt_rows = []
    for item in preview.candidates:
        if item.sell is None or item.buy is None:
            continue
        tt_rows.append(
            RakutenAuditRowRead(
                symbol=item.symbol,
                name=item.name,
                sell_date=item.sell.date,
                qty=item.sell.qty,
                sell_price=float(item.sell.price),
                buy_price_or_avg_cost=float(item.buy.price),
                tt_profit_jpy=_candidate_profit_jpy(item),
            )
        )

    realized_rows = _parse_realized_pl_csv(realized_content)
    realized_by_key: dict[tuple[str, str, int, int, int], list[RakutenAuditRowRead]] = {}
    realized_by_loose_key: dict[tuple[str, str, int, str], list[RakutenAuditRowRead]] = {}
    for row in realized_rows:
        key = _audit_key(row.symbol, row.sell_date, row.qty, row.sell_price, row.buy_price_or_avg_cost)
        realized_by_key.setdefault(key, []).append(row)
        loose_key = _audit_key_loose(row.symbol, row.sell_date, row.qty, row.sell_price)
        realized_by_loose_key.setdefault(loose_key, []).append(row)

    matched_count = 0
    pnl_mismatch: list[RakutenAuditRowRead] = []
    unmatched_tt: list[RakutenAuditRowRead] = []

    for row in tt_rows:
        key = _audit_key(row.symbol, row.sell_date, row.qty, row.sell_price, row.buy_price_or_avg_cost)
        bucket = realized_by_key.get(key) or []
        if not bucket:
            loose_key = _audit_key_loose(row.symbol, row.sell_date, row.qty, row.sell_price)
            loose_bucket = realized_by_loose_key.get(loose_key) or []
            if loose_bucket:
                realized = loose_bucket[0]
                row.rakuten_profit_jpy = realized.rakuten_profit_jpy
                row.reason_code = "buy_price_basis_mismatch"
                row.message = "買値基準が一致していないため、楽天平均取得価額とTTの建値がずれています。"
                pnl_mismatch.append(row)
            else:
                row.reason_code = "missing_in_tradehistory"
                row.message = "TT側にはありますが、楽天実現損益とは結びつきませんでした。"
                unmatched_tt.append(row)
            continue
        realized = bucket.pop(0)
        loose_key = _audit_key_loose(row.symbol, row.sell_date, row.qty, row.sell_price)
        if realized in realized_by_loose_key.get(loose_key, []):
            realized_by_loose_key[loose_key].remove(realized)
        matched_count += 1
        row.rakuten_profit_jpy = realized.rakuten_profit_jpy
        if round(float(row.tt_profit_jpy or 0.0), 2) != round(float(realized.rakuten_profit_jpy or 0.0), 2):
            row.reason_code = row.reason_code or "cost_breakdown_mismatch"
            row.message = "TT と楽天で実現損益が一致していません。コスト内訳または建玉情報が不足している可能性があります。"
            pnl_mismatch.append(row)

    missing_in_tt = []
    for rows in realized_by_key.values():
        for row in rows:
            if row.reason_code is None:
                row.reason_code = "missing_in_tradehistory"
            if row.message is None:
                row.message = "楽天の実現損益にはありますが、tradehistory からはTT側決済を再構成できませんでした。"
            missing_in_tt.append(row)

    tt_total = float(sum(Decimal(str(row.tt_profit_jpy or 0.0)) for row in tt_rows))
    rakuten_total = float(sum(Decimal(str(row.rakuten_profit_jpy or 0.0)) for row in realized_rows))
    gap = float(Decimal(str(tt_total)) - Decimal(str(rakuten_total)))

    symbol_diffs: dict[str, dict[str, object]] = {}
    for row in tt_rows:
        entry = symbol_diffs.setdefault(
            row.symbol,
            {"symbol": row.symbol, "name": row.name, "tt_profit_jpy": Decimal("0"), "rakuten_profit_jpy": Decimal("0")},
        )
        entry["tt_profit_jpy"] = Decimal(str(entry["tt_profit_jpy"])) + Decimal(str(row.tt_profit_jpy or 0.0))
    for row in realized_rows:
        entry = symbol_diffs.setdefault(
            row.symbol,
            {"symbol": row.symbol, "name": row.name, "tt_profit_jpy": Decimal("0"), "rakuten_profit_jpy": Decimal("0")},
        )
        entry["rakuten_profit_jpy"] = Decimal(str(entry["rakuten_profit_jpy"])) + Decimal(str(row.rakuten_profit_jpy or 0.0))

    top_symbol_diffs = []
    for entry in symbol_diffs.values():
        tt_profit = Decimal(str(entry["tt_profit_jpy"]))
        rakuten_profit = Decimal(str(entry["rakuten_profit_jpy"]))
        diff = tt_profit - rakuten_profit
        if diff == 0:
            continue
        top_symbol_diffs.append(
            {
                "symbol": str(entry["symbol"]),
                "name": entry["name"],
                "tt_profit_jpy": float(tt_profit),
                "rakuten_profit_jpy": float(rakuten_profit),
                "gap_jpy": float(diff),
            }
        )
    top_symbol_diffs.sort(key=lambda item: abs(float(item["gap_jpy"])), reverse=True)

    return RakutenImportAuditResponse(
        tt_total_jpy=tt_total,
        rakuten_total_jpy=rakuten_total,
        gap_jpy=gap,
        matched_count=matched_count,
        missing_in_tt=missing_in_tt,
        pnl_mismatch=pnl_mismatch,
        unmatched_tt=unmatched_tt,
        top_symbol_diffs=top_symbol_diffs[:10],
        reimport_hint="差額が残る場合は tradehistory の対象期間確認と、最新ロジックでの再取込を優先してください。",
    )
