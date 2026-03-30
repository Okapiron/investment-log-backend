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
    date: str
    qty: int
    price: Decimal
    fee: int
    build_date: Optional[str] = None
    build_price: Optional[Decimal] = None
    build_fee: int = 0
    is_credit_close: bool = False


@dataclass
class _AggregatedTrade:
    symbol: str
    name: str
    side: str
    date: str
    qty: int
    price: Decimal
    fee: int
    lines: list[int]
    row_signatures: list[str]
    build_date: Optional[str] = None
    build_price: Optional[Decimal] = None
    build_fee: int = 0
    is_credit_close: bool = False


@dataclass
class _OpenLot:
    symbol: str
    name: str
    date: str
    qty: int
    price: Decimal
    remaining_qty: int
    remaining_fee: int
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
    return any(marker in combined for marker in ("信用返済", "返済売", "売埋"))


def _is_supported_domestic_stock(row: dict[str, str], headers: dict[str, str]) -> bool:
    trade_type, credit_type, side_text = _trade_context(row, headers)
    market = _clean_text(row.get(headers.get("market", ""), ""))
    if trade_type:
        if "先物" in trade_type or "オプション" in trade_type or "投信" in trade_type:
            return False
        if "信用" in trade_type:
            combined = " ".join(part for part in (trade_type, credit_type, side_text) if part)
            if any(marker in combined for marker in ("売建", "新規売", "返済買")):
                return False
            if any(marker in combined for marker in ("買建", "新規買", "返済売", "新規", "返済")):
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
    if any(marker in combined for marker in ("買建", "新規買", "返済買")):
        return "buy"
    if any(marker in combined for marker in ("売建", "新規売", "返済売")):
        return "sell"
    return None


def _row_signature(raw: _RawCsvTrade) -> str:
    base = f"{raw.symbol}|{raw.name}|{raw.side}|{raw.date}|{raw.qty}|{raw.price}|{raw.fee}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _aggregate_rows(rows: list[_RawCsvTrade]) -> list[_AggregatedTrade]:
    grouped: dict[tuple[str, str, str, str, str, bool], list[_RawCsvTrade]] = {}
    for row in rows:
        key = (
            row.symbol,
            row.side,
            row.date,
            row.build_date or "",
            str(row.build_price or ""),
            bool(row.is_credit_close),
        )
        grouped.setdefault(key, []).append(row)

    aggregated: list[_AggregatedTrade] = []
    for (symbol, side, date, build_date, build_price, is_credit_close), items in grouped.items():
        total_qty = sum(item.qty for item in items)
        total_fee = sum(item.fee for item in items)
        total_build_fee = sum(item.build_fee for item in items)
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
                date=date,
                qty=total_qty,
                price=avg_price,
                fee=total_fee,
                lines=lines,
                row_signatures=row_signatures,
                build_date=build_date or None,
                build_price=avg_build_price if build_price else None,
                build_fee=total_build_fee,
                is_credit_close=is_credit_close,
            )
        )
    aggregated.sort(key=lambda item: (item.symbol, item.date, 0 if item.side == "buy" else 1))
    return aggregated


def _position_key(symbol: str, buy_date: str, buy_price: Decimal) -> str:
    base = "|".join(
        [
            "rakuten",
            "JP",
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


def _find_matching_open_lot(open_buys: list[_OpenLot], sell: _AggregatedTrade) -> Optional[_OpenLot]:
    if sell.build_date and sell.build_price:
        for lot in open_buys:
            if lot.date == sell.build_date and lot.price == sell.build_price:
                return lot
    return open_buys[0] if open_buys else None


def _synthetic_open_lot_from_credit_close(sell: _AggregatedTrade) -> _OpenLot:
    buy_date = sell.build_date or sell.date
    buy_price = Decimal(str(sell.build_price or sell.price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    source_key = _position_key(sell.symbol, buy_date, buy_price)
    return _OpenLot(
        symbol=sell.symbol,
        name=sell.name,
        date=buy_date,
        qty=sell.qty,
        price=buy_price,
        remaining_qty=sell.qty,
        remaining_fee=max(0, int(sell.build_fee or 0)),
        lines=[],
        row_signatures=[f"synthetic:{source_key}"],
        source_position_key=source_key,
    )


def _candidate_from_buy_sell(
    buy_lot: _OpenLot,
    sell: _AggregatedTrade,
    matched_qty: int,
    buy_fee: int,
    sell_fee: int,
    *,
    is_partial_exit: bool,
    remaining_qty_after_sell: int,
) -> ImportTradeCandidateRead:
    sequence = buy_lot.next_sequence
    buy_lot.next_sequence += 1
    sell_preview = _AggregatedTrade(
        symbol=sell.symbol,
        name=sell.name,
        side=sell.side,
        date=sell.date,
        qty=matched_qty,
        price=sell.price,
        fee=sell_fee,
        lines=sell.lines,
        row_signatures=sell.row_signatures,
    )
    return ImportTradeCandidateRead(
        source_signature=_candidate_signature(
            buy_lot.symbol,
            buy_lot.date,
            matched_qty,
            buy_lot.price,
            buy_fee,
            buy_lot.source_position_key,
            sequence,
            buy_lot.row_signatures,
            sell=sell_preview,
        ),
        source_position_key=buy_lot.source_position_key,
        source_lot_sequence=sequence,
        symbol=buy_lot.symbol,
        name=buy_lot.name or sell.name or buy_lot.symbol,
        market="JP",
        buy=ImportFillPreviewRead(date=buy_lot.date, price=float(buy_lot.price), qty=matched_qty, fee=buy_fee),
        sell=ImportFillPreviewRead(date=sell.date, price=float(sell.price), qty=matched_qty, fee=sell_fee),
        source_lines=sorted(set([*buy_lot.lines, *sell.lines])),
        already_imported=False,
        is_partial_exit=is_partial_exit,
        remaining_qty_after_sell=max(0, remaining_qty_after_sell),
    )


def _candidate_from_open_lot(buy_lot: _OpenLot, *, is_partial_exit: bool) -> ImportTradeCandidateRead:
    sequence = buy_lot.next_sequence
    buy_lot.next_sequence += 1
    return ImportTradeCandidateRead(
        source_signature=_candidate_signature(
            buy_lot.symbol,
            buy_lot.date,
            buy_lot.remaining_qty,
            buy_lot.price,
            buy_lot.remaining_fee,
            buy_lot.source_position_key,
            sequence,
            buy_lot.row_signatures,
        ),
        source_position_key=buy_lot.source_position_key,
        source_lot_sequence=sequence,
        symbol=buy_lot.symbol,
        name=buy_lot.name or buy_lot.symbol,
        market="JP",
        buy=ImportFillPreviewRead(
            date=buy_lot.date,
            price=float(buy_lot.price),
            qty=buy_lot.remaining_qty,
            fee=buy_lot.remaining_fee,
        ),
        sell=None,
        source_lines=sorted(buy_lot.lines),
        already_imported=False,
        is_partial_exit=is_partial_exit,
        remaining_qty_after_sell=buy_lot.remaining_qty,
    )


def _pair_round_trips(rows: list[_AggregatedTrade]) -> tuple[list[ImportTradeCandidateRead], list[ImportIssueRead], list[ImportIssueRead]]:
    candidates: list[ImportTradeCandidateRead] = []
    skipped: list[ImportIssueRead] = []
    errors: list[ImportIssueRead] = []
    by_symbol: dict[str, list[_AggregatedTrade]] = {}
    for row in rows:
        by_symbol.setdefault(row.symbol, []).append(row)

    for symbol, items in by_symbol.items():
        open_buys: list[_OpenLot] = []
        for item in sorted(items, key=lambda row: (row.date, 0 if row.side == "buy" else 1)):
            if item.side == "buy":
                open_buys.append(
                    _OpenLot(
                        symbol=item.symbol,
                        name=item.name,
                        date=item.date,
                        qty=item.qty,
                        price=item.price,
                        remaining_qty=item.qty,
                        remaining_fee=item.fee,
                        lines=item.lines,
                        row_signatures=item.row_signatures,
                        source_position_key=_position_key(item.symbol, item.date, item.price),
                    )
                )
                continue

            remaining_sell_qty = item.qty
            remaining_sell_fee = item.fee

            while remaining_sell_qty > 0:
                buy_lot = _find_matching_open_lot(open_buys, item)
                if buy_lot is None:
                    if item.is_credit_close and item.build_date and item.build_price:
                        buy_lot = _synthetic_open_lot_from_credit_close(item)
                    else:
                        skipped.append(
                            ImportIssueRead(
                                line=item.lines[0] if item.lines else None,
                                code="sell_without_buy",
                                message=f"{symbol} の売却に対応する購入が見つかりません。",
                            )
                        )
                        break

                lot_qty_before = buy_lot.remaining_qty
                sell_qty_before = remaining_sell_qty
                matched_qty = min(lot_qty_before, sell_qty_before)
                buy_fee_portion = _allocate_fee_portion(buy_lot.remaining_fee, matched_qty, lot_qty_before)
                sell_fee_portion = _allocate_fee_portion(remaining_sell_fee, matched_qty, sell_qty_before)
                remaining_after_sell = max(0, lot_qty_before - matched_qty)
                is_partial_exit = buy_lot.qty != matched_qty or item.qty != matched_qty

                candidates.append(
                    _candidate_from_buy_sell(
                        buy_lot,
                        item,
                        matched_qty,
                        buy_fee_portion,
                        sell_fee_portion,
                        is_partial_exit=is_partial_exit,
                        remaining_qty_after_sell=remaining_after_sell,
                    )
                )

                buy_lot.remaining_qty -= matched_qty
                buy_lot.remaining_fee = max(0, buy_lot.remaining_fee - buy_fee_portion)
                remaining_sell_qty -= matched_qty
                remaining_sell_fee = max(0, remaining_sell_fee - sell_fee_portion)

                if buy_lot in open_buys and buy_lot.remaining_qty <= 0:
                    open_buys.remove(buy_lot)

        for buy_lot in open_buys:
            candidates.append(_candidate_from_open_lot(buy_lot, is_partial_exit=buy_lot.remaining_qty != buy_lot.qty))

    candidates.sort(
        key=lambda item: (
            item.buy.date,
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
        qty = _parse_jp_int(row.get(headers["qty"]))
        price = _parse_jp_decimal(row.get(headers["price"]))
        is_credit_close = _is_credit_close_row(row, headers)
        build_date = _parse_date(row.get(headers.get("build_date", "")))
        build_price = _parse_jp_decimal(row.get(headers.get("build_price", "")))
        build_fee = (_parse_jp_int(row.get(headers.get("build_fee", ""))) or 0) + (
            _parse_jp_int(row.get(headers.get("build_fee_tax", ""))) or 0
        )
        fee = (_parse_jp_int(row.get(headers.get("fee", ""))) or 0) + (_parse_jp_int(row.get(headers.get("other_fee", ""))) or 0)
        fee += _parse_jp_int(row.get(headers.get("tax_fee", ""))) or 0

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
                date=date,
                qty=qty,
                price=price,
                fee=max(0, fee),
                build_date=build_date,
                build_price=build_price,
                build_fee=max(0, build_fee),
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
            )
        )
    return rows


def _candidate_profit_jpy(item: ImportTradeCandidateRead) -> float:
    if item.sell is None:
        return 0.0
    profit = (Decimal(str(item.sell.price)) - Decimal(str(item.buy.price))) * Decimal(item.buy.qty)
    profit -= Decimal(item.buy.fee or 0)
    profit -= Decimal(item.sell.fee or 0)
    return float(profit.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _audit_key(symbol: str, sell_date: str, qty: int, sell_price: float, buy_price: float) -> tuple[str, str, int, str, str]:
    return (
        symbol,
        sell_date,
        int(qty),
        _price_text(Decimal(str(sell_price))),
        _price_text(Decimal(str(buy_price))),
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
        if item.sell is None:
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
    for row in realized_rows:
        key = _audit_key(row.symbol, row.sell_date, row.qty, row.sell_price, row.buy_price_or_avg_cost)
        realized_by_key.setdefault(key, []).append(row)

    matched_count = 0
    pnl_mismatch: list[RakutenAuditRowRead] = []
    unmatched_tt: list[RakutenAuditRowRead] = []

    for row in tt_rows:
        key = _audit_key(row.symbol, row.sell_date, row.qty, row.sell_price, row.buy_price_or_avg_cost)
        bucket = realized_by_key.get(key) or []
        if not bucket:
            unmatched_tt.append(row)
            continue
        realized = bucket.pop(0)
        matched_count += 1
        row.rakuten_profit_jpy = realized.rakuten_profit_jpy
        if round(float(row.tt_profit_jpy or 0.0), 2) != round(float(realized.rakuten_profit_jpy or 0.0), 2):
            row.message = "TT と楽天で実現損益が一致していません。"
            pnl_mismatch.append(row)

    missing_in_tt = []
    for rows in realized_by_key.values():
        missing_in_tt.extend(rows)

    tt_total = float(sum(Decimal(str(row.tt_profit_jpy or 0.0)) for row in tt_rows))
    rakuten_total = float(sum(Decimal(str(row.rakuten_profit_jpy or 0.0)) for row in realized_rows))
    gap = float(Decimal(str(tt_total)) - Decimal(str(rakuten_total)))

    return RakutenImportAuditResponse(
        tt_total_jpy=tt_total,
        rakuten_total_jpy=rakuten_total,
        gap_jpy=gap,
        matched_count=matched_count,
        missing_in_tt=missing_in_tt,
        pnl_mismatch=pnl_mismatch,
        unmatched_tt=unmatched_tt,
        reimport_hint="信用返済の建玉情報を反映した再取込で差分の解消を狙えます。",
    )
