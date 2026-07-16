import csv
import json
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import requests
from eth_abi import decode
from eth_utils import keccak
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

getcontext().prec = 60

CONTRACT_ADDRESS = os.getenv(
    "CONTRACT_ADDRESS",
    "0x18b2a687610328590bc8f2e5fedde3b582a49cda",
).strip()
RPC_URL = os.getenv("BSC_RPC_URL", "").strip()
START_EPOCH = max(1, int(os.getenv("START_EPOCH", "1")))
END_EPOCH_ENV = os.getenv("END_EPOCH", "").strip()
BATCH_SIZE = max(1, min(500, int(os.getenv("BATCH_SIZE", "100"))))
BATCH_DELAY = max(0.0, float(os.getenv("BATCH_DELAY_SECONDS", "0.15")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "6")))
FALLBACK_WORKERS = max(1, min(32, int(os.getenv("FALLBACK_WORKERS", "10"))))
AUTO_START = os.getenv("AUTO_START", "false").lower() in {"1", "true", "yes", "on"}
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()

DATA_DIR = Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "pancakeswap_rounds.sqlite3"
FULL_CSV_PATH = DATA_DIR / "pancakeswap_bnb_all_rounds.csv"
VALID_CSV_PATH = DATA_DIR / "pancakeswap_bnb_valid_rounds.csv"
OUTCOMES_CSV_PATH = DATA_DIR / "pancakeswap_bnb_outcomes.csv"

CURRENT_EPOCH_SELECTOR = "0x" + keccak(text="currentEpoch()")[:4].hex()
ROUNDS_SELECTOR = keccak(text="rounds(uint256)")[:4].hex()
ROUND_TYPES = [
    "uint256", "uint256", "uint256", "uint256",
    "int256", "int256",
    "uint256", "uint256", "uint256", "uint256", "uint256",
    "uint256", "uint256", "bool",
]

app = FastAPI(title="PancakeSwap Prediction History Downloader", version="1.0.0")
stop_event = threading.Event()
worker_lock = threading.Lock()
worker_thread: threading.Thread | None = None
state_lock = threading.Lock()
state: dict[str, Any] = {
    "running": False,
    "completed": False,
    "message": "Ожидание запуска",
    "current_contract_epoch": None,
    "start_epoch": START_EPOCH,
    "end_epoch": None,
    "next_epoch": None,
    "processed": 0,
    "saved_total": 0,
    "valid_up_down": 0,
    "invalid_or_unresolved": 0,
    "progress_percent": 0.0,
    "last_error": None,
    "updated_at_utc": None,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamp_to_iso(value: int | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except (ValueError, OSError, OverflowError):
        return ""


def require_token(token: str | None) -> None:
    if ADMIN_TOKEN and token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Неверный ADMIN_TOKEN")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rounds (
                epoch INTEGER PRIMARY KEY,
                start_timestamp INTEGER,
                lock_timestamp INTEGER,
                close_timestamp INTEGER,
                lock_price_raw TEXT,
                close_price_raw TEXT,
                lock_price TEXT,
                close_price TEXT,
                lock_oracle_id TEXT,
                close_oracle_id TEXT,
                total_amount_wei TEXT,
                total_amount_bnb TEXT,
                bull_amount_wei TEXT,
                bull_amount_bnb TEXT,
                bear_amount_wei TEXT,
                bear_amount_bnb TEXT,
                reward_base_cal_amount_wei TEXT,
                reward_amount_wei TEXT,
                oracle_called INTEGER,
                status TEXT,
                outcome TEXT,
                move_raw TEXT,
                move_usd TEXT,
                move_percent TEXT,
                downloaded_at_utc TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rounds_status ON rounds(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rounds_outcome ON rounds(outcome)")


def set_meta(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, json.dumps(value)),
    )


def get_meta(key: str, default: Any = None) -> Any:
    with get_connection() as conn:
        row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row[0])
    except json.JSONDecodeError:
        return default


def rpc_post(payload: Any, timeout: int = 120) -> Any:
    if not RPC_URL:
        raise RuntimeError("Переменная BSC_RPC_URL не задана")
    response = requests.post(RPC_URL, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def get_current_epoch() -> int:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": CONTRACT_ADDRESS, "data": CURRENT_EPOCH_SELECTOR}, "latest"],
    }
    data = rpc_post(payload)
    if "error" in data:
        raise RuntimeError(f"RPC currentEpoch error: {data['error']}")
    result = data.get("result")
    if not result or result == "0x":
        raise RuntimeError("RPC вернул пустой currentEpoch")
    return int(result, 16)


def encode_round_call(epoch: int) -> str:
    return "0x" + ROUNDS_SELECTOR + epoch.to_bytes(32, byteorder="big").hex()


def decode_round(epoch_requested: int, result_hex: str) -> dict[str, Any]:
    if not result_hex or result_hex == "0x":
        raise ValueError(f"Пустой ответ rounds({epoch_requested})")
    raw = bytes.fromhex(result_hex[2:] if result_hex.startswith("0x") else result_hex)
    values = decode(ROUND_TYPES, raw)
    (
        epoch,
        start_timestamp,
        lock_timestamp,
        close_timestamp,
        lock_price,
        close_price,
        lock_oracle_id,
        close_oracle_id,
        total_amount,
        bull_amount,
        bear_amount,
        reward_base_cal_amount,
        reward_amount,
        oracle_called,
    ) = values

    epoch = int(epoch)
    start_timestamp = int(start_timestamp)
    lock_timestamp = int(lock_timestamp)
    close_timestamp = int(close_timestamp)
    lock_price = int(lock_price)
    close_price = int(close_price)
    lock_oracle_id = int(lock_oracle_id)
    close_oracle_id = int(close_oracle_id)
    total_amount = int(total_amount)
    bull_amount = int(bull_amount)
    bear_amount = int(bear_amount)
    reward_base_cal_amount = int(reward_base_cal_amount)
    reward_amount = int(reward_amount)
    oracle_called = bool(oracle_called)

    if epoch == 0:
        status = "EMPTY"
        outcome = ""
    elif not oracle_called:
        status = "UNRESOLVED_OR_CANCELLED"
        outcome = ""
    elif lock_price <= 0 or close_price <= 0:
        status = "INVALID_PRICE"
        outcome = ""
    elif close_price > lock_price:
        status = "CLOSED"
        outcome = "UP"
    elif close_price < lock_price:
        status = "CLOSED"
        outcome = "DOWN"
    else:
        status = "TIE"
        outcome = "TIE"

    price_scale = Decimal(10) ** 8
    amount_scale = Decimal(10) ** 18
    lock_price_dec = Decimal(lock_price) / price_scale
    close_price_dec = Decimal(close_price) / price_scale
    move_raw = close_price - lock_price
    move_usd = Decimal(move_raw) / price_scale
    move_percent = Decimal(0)
    if lock_price:
        move_percent = Decimal(move_raw) * Decimal(100) / Decimal(lock_price)

    return {
        "epoch": epoch_requested if epoch == 0 else epoch,
        "start_timestamp": start_timestamp,
        "lock_timestamp": lock_timestamp,
        "close_timestamp": close_timestamp,
        "lock_price_raw": str(lock_price),
        "close_price_raw": str(close_price),
        "lock_price": format(lock_price_dec, "f"),
        "close_price": format(close_price_dec, "f"),
        "lock_oracle_id": str(lock_oracle_id),
        "close_oracle_id": str(close_oracle_id),
        "total_amount_wei": str(total_amount),
        "total_amount_bnb": format(Decimal(total_amount) / amount_scale, "f"),
        "bull_amount_wei": str(bull_amount),
        "bull_amount_bnb": format(Decimal(bull_amount) / amount_scale, "f"),
        "bear_amount_wei": str(bear_amount),
        "bear_amount_bnb": format(Decimal(bear_amount) / amount_scale, "f"),
        "reward_base_cal_amount_wei": str(reward_base_cal_amount),
        "reward_amount_wei": str(reward_amount),
        "oracle_called": 1 if oracle_called else 0,
        "status": status,
        "outcome": outcome,
        "move_raw": str(move_raw),
        "move_usd": format(move_usd, "f"),
        "move_percent": format(move_percent, "f"),
        "downloaded_at_utc": utc_now_iso(),
    }


def fetch_one_round(epoch: int) -> dict[str, Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": epoch,
        "method": "eth_call",
        "params": [{"to": CONTRACT_ADDRESS, "data": encode_round_call(epoch)}, "latest"],
    }
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            data = rpc_post(payload)
            if isinstance(data, dict) and "error" in data:
                raise RuntimeError(str(data["error"]))
            return decode_round(epoch, data["result"])
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(min(10.0, 0.75 * (2 ** attempt)))
    raise RuntimeError(f"Не удалось загрузить epoch {epoch}: {last_error}")


def fetch_batch(epochs: list[int]) -> list[dict[str, Any]]:
    payload = [
        {
            "jsonrpc": "2.0",
            "id": epoch,
            "method": "eth_call",
            "params": [{"to": CONTRACT_ADDRESS, "data": encode_round_call(epoch)}, "latest"],
        }
        for epoch in epochs
    ]

    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            data = rpc_post(payload)
            if not isinstance(data, list):
                raise RuntimeError("RPC не поддержал JSON-RPC batch")
            by_id = {int(item["id"]): item for item in data if isinstance(item, dict) and "id" in item}
            rows: list[dict[str, Any]] = []
            missing: list[int] = []
            for epoch in epochs:
                item = by_id.get(epoch)
                if not item or "error" in item or not item.get("result"):
                    missing.append(epoch)
                    continue
                rows.append(decode_round(epoch, item["result"]))
            if missing:
                with ThreadPoolExecutor(max_workers=FALLBACK_WORKERS) as executor:
                    futures = {executor.submit(fetch_one_round, epoch): epoch for epoch in missing}
                    for future in as_completed(futures):
                        rows.append(future.result())
            rows.sort(key=lambda row: row["epoch"])
            return rows
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if "batch" in str(exc).lower():
                break
            time.sleep(min(10.0, 0.75 * (2 ** attempt)))

    # Полный fallback, если провайдер не принимает batch-запросы.
    rows = []
    try:
        with ThreadPoolExecutor(max_workers=FALLBACK_WORKERS) as executor:
            futures = {executor.submit(fetch_one_round, epoch): epoch for epoch in epochs}
            for future in as_completed(futures):
                rows.append(future.result())
        rows.sort(key=lambda row: row["epoch"])
        return rows
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Ошибка batch и fallback: {last_error}; {exc}") from exc


ROUND_COLUMNS = [
    "epoch", "start_timestamp", "lock_timestamp", "close_timestamp",
    "lock_price_raw", "close_price_raw", "lock_price", "close_price",
    "lock_oracle_id", "close_oracle_id",
    "total_amount_wei", "total_amount_bnb",
    "bull_amount_wei", "bull_amount_bnb",
    "bear_amount_wei", "bear_amount_bnb",
    "reward_base_cal_amount_wei", "reward_amount_wei",
    "oracle_called", "status", "outcome",
    "move_raw", "move_usd", "move_percent", "downloaded_at_utc",
]


def save_rows(rows: list[dict[str, Any]], next_epoch: int) -> None:
    placeholders = ",".join("?" for _ in ROUND_COLUMNS)
    update_clause = ",".join(
        f"{column}=excluded.{column}" for column in ROUND_COLUMNS if column != "epoch"
    )
    sql = (
        f"INSERT INTO rounds ({','.join(ROUND_COLUMNS)}) VALUES ({placeholders}) "
        f"ON CONFLICT(epoch) DO UPDATE SET {update_clause}"
    )
    with get_connection() as conn:
        conn.execute("BEGIN")
        conn.executemany(sql, [[row[column] for column in ROUND_COLUMNS] for row in rows])
        set_meta(conn, "next_epoch", next_epoch)
        set_meta(conn, "last_saved_at_utc", utc_now_iso())
        conn.commit()


def get_counts() -> tuple[int, int, int]:
    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM rounds").fetchone()[0]
        valid = conn.execute(
            "SELECT COUNT(*) FROM rounds WHERE status='CLOSED' AND outcome IN ('UP','DOWN')"
        ).fetchone()[0]
        invalid = total - valid
    return int(total), int(valid), int(invalid)


def update_state(**kwargs: Any) -> None:
    with state_lock:
        state.update(kwargs)
        state["updated_at_utc"] = utc_now_iso()


def resolve_start_epoch(reset: bool) -> int:
    if reset:
        with get_connection() as conn:
            conn.execute("DELETE FROM rounds")
            conn.execute("DELETE FROM metadata")
            conn.commit()
        return START_EPOCH
    next_epoch = get_meta("next_epoch")
    if isinstance(next_epoch, int) and next_epoch >= START_EPOCH:
        return next_epoch
    with get_connection() as conn:
        max_epoch = conn.execute("SELECT MAX(epoch) FROM rounds").fetchone()[0]
    return max(START_EPOCH, int(max_epoch) + 1 if max_epoch is not None else START_EPOCH)


def download_worker(reset: bool = False) -> None:
    try:
        stop_event.clear()
        update_state(
            running=True,
            completed=False,
            message="Подключение к BNB Smart Chain",
            last_error=None,
        )
        current_contract_epoch = get_current_epoch()
        end_epoch = int(END_EPOCH_ENV) if END_EPOCH_ENV else current_contract_epoch - 2
        if end_epoch < START_EPOCH:
            raise RuntimeError("END_EPOCH меньше START_EPOCH")
        next_epoch = resolve_start_epoch(reset)
        total_target = end_epoch - START_EPOCH + 1
        update_state(
            current_contract_epoch=current_contract_epoch,
            start_epoch=START_EPOCH,
            end_epoch=end_epoch,
            next_epoch=next_epoch,
            message="Загрузка истории",
        )

        while next_epoch <= end_epoch and not stop_event.is_set():
            batch_end = min(end_epoch, next_epoch + BATCH_SIZE - 1)
            epochs = list(range(next_epoch, batch_end + 1))
            rows = fetch_batch(epochs)
            if len(rows) != len(epochs):
                raise RuntimeError(
                    f"Неполный пакет {next_epoch}-{batch_end}: получено {len(rows)} из {len(epochs)}"
                )
            save_rows(rows, batch_end + 1)
            next_epoch = batch_end + 1
            total, valid, invalid = get_counts()
            processed_range = max(0, min(end_epoch, next_epoch - 1) - START_EPOCH + 1)
            progress = round(processed_range * 100 / total_target, 4) if total_target else 100.0
            update_state(
                next_epoch=next_epoch,
                processed=processed_range,
                saved_total=total,
                valid_up_down=valid,
                invalid_or_unresolved=invalid,
                progress_percent=progress,
                message=f"Сохранено до epoch {batch_end}",
            )
            if BATCH_DELAY:
                time.sleep(BATCH_DELAY)

        total, valid, invalid = get_counts()
        if stop_event.is_set():
            update_state(
                running=False,
                completed=False,
                saved_total=total,
                valid_up_down=valid,
                invalid_or_unresolved=invalid,
                message="Остановлено. Повторный запуск продолжит с checkpoint.",
            )
        else:
            with get_connection() as conn:
                set_meta(conn, "completed_end_epoch", end_epoch)
                set_meta(conn, "completed_at_utc", utc_now_iso())
                conn.commit()
            update_state(
                running=False,
                completed=True,
                saved_total=total,
                valid_up_down=valid,
                invalid_or_unresolved=invalid,
                progress_percent=100.0,
                message="История полностью загружена",
            )
    except Exception as exc:  # noqa: BLE001
        total, valid, invalid = get_counts()
        update_state(
            running=False,
            completed=False,
            saved_total=total,
            valid_up_down=valid,
            invalid_or_unresolved=invalid,
            last_error=str(exc),
            message="Загрузка остановлена из-за ошибки. Повторный запуск продолжит с checkpoint.",
        )


def start_worker(reset: bool = False) -> None:
    global worker_thread
    with worker_lock:
        if worker_thread and worker_thread.is_alive():
            raise HTTPException(status_code=409, detail="Загрузка уже запущена")
        worker_thread = threading.Thread(target=download_worker, args=(reset,), daemon=True)
        worker_thread.start()


def export_csv(kind: str) -> Path:
    if kind == "full":
        path = FULL_CSV_PATH
        where = ""
        columns = [
            "epoch", "start_timestamp", "lock_timestamp", "close_timestamp",
            "lock_price_raw", "close_price_raw", "lock_price", "close_price",
            "lock_oracle_id", "close_oracle_id",
            "total_amount_wei", "total_amount_bnb",
            "bull_amount_wei", "bull_amount_bnb",
            "bear_amount_wei", "bear_amount_bnb",
            "reward_base_cal_amount_wei", "reward_amount_wei",
            "oracle_called", "status", "outcome",
            "move_raw", "move_usd", "move_percent", "downloaded_at_utc",
        ]
    elif kind == "valid":
        path = VALID_CSV_PATH
        where = "WHERE status='CLOSED' AND outcome IN ('UP','DOWN')"
        columns = [
            "epoch", "start_timestamp", "lock_timestamp", "close_timestamp",
            "lock_price", "close_price", "outcome", "move_usd", "move_percent",
            "total_amount_bnb", "bull_amount_bnb", "bear_amount_bnb",
        ]
    elif kind == "outcomes":
        path = OUTCOMES_CSV_PATH
        where = "WHERE status='CLOSED' AND outcome IN ('UP','DOWN')"
        columns = ["epoch", "lock_timestamp", "close_timestamp", "lock_price", "close_price", "outcome"]
    else:
        raise ValueError("Неизвестный тип CSV")

    temp_path = path.with_suffix(path.suffix + ".tmp")
    query = f"SELECT {','.join(columns)} FROM rounds {where} ORDER BY epoch"
    with get_connection() as conn, temp_path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        header = []
        for column in columns:
            header.append(column)
            if column == "start_timestamp":
                header.append("start_datetime_utc")
            elif column == "lock_timestamp":
                header.append("lock_datetime_utc")
            elif column == "close_timestamp":
                header.append("close_datetime_utc")
        writer.writerow(header)
        cursor = conn.execute(query)
        for db_row in cursor:
            row = []
            for column, value in zip(columns, db_row):
                row.append(value)
                if column in {"start_timestamp", "lock_timestamp", "close_timestamp"}:
                    row.append(timestamp_to_iso(int(value) if value else None))
            writer.writerow(row)
    temp_path.replace(path)
    return path


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    total, valid, invalid = get_counts()
    update_state(saved_total=total, valid_up_down=valid, invalid_or_unresolved=invalid)
    if AUTO_START and RPC_URL:
        try:
            start_worker(reset=False)
        except HTTPException:
            pass


@app.get("/", response_class=HTMLResponse)
def home(token: str | None = Query(default=None)) -> HTMLResponse:
    token_js = json.dumps(token or "")
    html = f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>PancakeSwap History Downloader</title>
      <style>
        body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 32px auto; padding: 0 18px; background:#f6f7f9; color:#161616; }}
        .card {{ background:white; border-radius:16px; padding:22px; box-shadow:0 4px 20px rgba(0,0,0,.08); margin-bottom:18px; }}
        h1 {{ margin-top:0; }}
        pre {{ white-space:pre-wrap; word-break:break-word; background:#111827; color:#e5e7eb; padding:16px; border-radius:12px; }}
        button, a.btn {{ display:inline-block; margin:5px 6px 5px 0; padding:11px 15px; border:0; border-radius:10px; background:#111827; color:white; text-decoration:none; cursor:pointer; }}
        .danger {{ background:#991b1b; }}
        .secondary {{ background:#4b5563; }}
      </style>
    </head>
    <body>
      <div class="card">
        <h1>PancakeSwap BNB Prediction — история</h1>
        <p>Страница автоматически обновляет статус. Файлы формируются из SQLite без потери прогресса.</p>
        <button onclick="action('/start')">Запустить / продолжить</button>
        <button class="secondary" onclick="action('/stop')">Остановить</button>
        <button class="danger" onclick="resetAll()">Удалить и начать заново</button>
      </div>
      <div class="card"><pre id="status">Загрузка статуса…</pre></div>
      <div class="card">
        <a class="btn" id="full" href="#">Скачать все раунды CSV</a>
        <a class="btn" id="valid" href="#">Скачать валидные раунды CSV</a>
        <a class="btn" id="outcomes" href="#">Скачать только UP/DOWN CSV</a>
        <a class="btn secondary" id="db" href="#">Скачать SQLite</a>
      </div>
      <script>
        const token = {token_js};
        const q = token ? ('?token=' + encodeURIComponent(token)) : '';
        document.getElementById('full').href = '/download/full.csv' + q;
        document.getElementById('valid').href = '/download/valid.csv' + q;
        document.getElementById('outcomes').href = '/download/outcomes.csv' + q;
        document.getElementById('db').href = '/download/database' + q;
        async function refresh() {{
          const r = await fetch('/status');
          document.getElementById('status').textContent = JSON.stringify(await r.json(), null, 2);
        }}
        async function action(path) {{
          const r = await fetch(path + q, {{method:'POST'}});
          alert(JSON.stringify(await r.json(), null, 2));
          refresh();
        }}
        async function resetAll() {{
          if (!confirm('Удалить всю уже загруженную историю и начать с epoch 1?')) return;
          const sep = q ? '&' : '?';
          const r = await fetch('/start' + q + sep + 'reset=true', {{method:'POST'}});
          alert(JSON.stringify(await r.json(), null, 2));
          refresh();
        }}
        refresh(); setInterval(refresh, 5000);
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/status")
def get_status() -> JSONResponse:
    total, valid, invalid = get_counts()
    with state_lock:
        payload = dict(state)
    payload.update({
        "contract_address": CONTRACT_ADDRESS,
        "rpc_configured": bool(RPC_URL),
        "database_path": str(DB_PATH),
        "saved_total": total,
        "valid_up_down": valid,
        "invalid_or_unresolved": invalid,
        "batch_size": BATCH_SIZE,
    })
    return JSONResponse(payload)


@app.post("/start")
def start_download(
    reset: bool = Query(default=False),
    token: str | None = Query(default=None),
) -> JSONResponse:
    require_token(token)
    start_worker(reset=reset)
    return JSONResponse({"ok": True, "message": "Загрузка запущена", "reset": reset})


@app.post("/stop")
def stop_download(token: str | None = Query(default=None)) -> JSONResponse:
    require_token(token)
    stop_event.set()
    return JSONResponse({"ok": True, "message": "Остановка запрошена; текущий пакет будет завершён"})


@app.get("/download/full.csv")
def download_full(token: str | None = Query(default=None)) -> FileResponse:
    require_token(token)
    path = export_csv("full")
    return FileResponse(path, filename=path.name, media_type="text/csv")


@app.get("/download/valid.csv")
def download_valid(token: str | None = Query(default=None)) -> FileResponse:
    require_token(token)
    path = export_csv("valid")
    return FileResponse(path, filename=path.name, media_type="text/csv")


@app.get("/download/outcomes.csv")
def download_outcomes(token: str | None = Query(default=None)) -> FileResponse:
    require_token(token)
    path = export_csv("outcomes")
    return FileResponse(path, filename=path.name, media_type="text/csv")


@app.get("/download/database")
def download_database(token: str | None = Query(default=None)) -> FileResponse:
    require_token(token)
    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="База ещё не создана")
    return FileResponse(DB_PATH, filename=DB_PATH.name, media_type="application/x-sqlite3")


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}
