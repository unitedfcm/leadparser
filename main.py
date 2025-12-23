"""
main.py — Bronze Star inbound email parser -> Granot poster (FIXED)

Fixes:
- Robust line-by-line parsing (no regex group bleed)
- Correct dstate/dzip mapping (no more "MOVING STATE"/"Moving zip")
- Email extraction cleaned (pulls first real email)
- Works with JSON, form-data, or raw text

Endpoints:
- GET  /health
- POST /inbound/bronze-email
"""

import os
import re
import json
from datetime import datetime
from typing import Dict, Any, Optional

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

GRANOT_BASE_URL = "https://lead.hellomoving.com/LEADSGWHTTP.lidgw"


# ---------------------------
# Helpers
# ---------------------------

EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)

KEY_ALIASES = {
    "email": ["email"],
    "first_name": ["first name", "firstname", "first"],
    "last_name": ["last name", "lastname", "last"],
    "phone": ["phone", "phone number", "tel", "telephone"],
    "move_date": ["move date", "move day", "moving date", "date"],
    "origin_city": ["origin city", "from city", "pickup city"],
    "origin_state": ["origin state", "from state", "pickup state"],
    "origin_zip": ["origin zip", "from zip", "pickup zip", "origin zipcode", "origin postal code"],
    "dest_city": ["moving city", "destination city", "to city", "dropoff city"],
    "dest_state": ["moving state", "destination state", "to state", "dropoff state"],
    "dest_zip": ["moving zip", "destination zip", "to zip", "dropoff zip", "destination zipcode", "destination postal code"],
    "bedrooms": ["number of bedrooms", "bedrooms", "br"],
    "home_type": ["home type", "housing type", "type"],
}


def _clean_text(s: str) -> str:
    if not s:
        return ""
    # normalize HTML-ish artifacts
    s = s.replace("&nbsp;", " ")
    s = re.sub(r"<mailto:.*?>", "", s, flags=re.IGNORECASE)
    # normalize line endings
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # collapse weird double spaces
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def _parse_move_date(raw: str) -> str:
    """
    Incoming often DD/MM/YYYY (23/12/2025).
    Granot wants MM/DD/YYYY.
    """
    raw = (raw or "").strip()
    if not raw:
        return ""

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return ""


def _servtypeid(ostate: str, dstate: str) -> str:
    if ostate and dstate and ostate.upper() == dstate.upper():
        return "101"  # local
    return "102"  # long distance / default


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalize_state(s: str) -> str:
    s = (s or "").strip().upper()
    # keep just 2-letter states if present
    if len(s) >= 2:
        m = re.search(r"\b([A-Z]{2})\b", s)
        if m:
            return m.group(1)
    return s[:2] if len(s) >= 2 else ""


def _first_email(text: str) -> str:
    m = EMAIL_RE.search(text or "")
    return m.group(1).strip() if m else ""


def _build_kv_from_lines(text: str) -> Dict[str, str]:
    """
    Parses:
      Key: Value
    into a dict with lowercase keys.
    Only uses the first ':' per line.
    """
    kv: Dict[str, str] = {}
    for raw_line in (text or "").split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip().lower()
        v = v.strip()
        if not k:
            continue
        # keep first occurrence (lead emails sometimes repeat)
        if k not in kv:
            kv[k] = v
    return kv


def _get_by_alias(kv: Dict[str, str], canonical: str) -> str:
    for alias in KEY_ALIASES.get(canonical, []):
        if alias in kv and kv[alias]:
            return kv[alias].strip()
    return ""


def parse_lead_from_text(text: str) -> Dict[str, str]:
    t = _clean_text(text)
    kv = _build_kv_from_lines(t)

    # Pull raw values from kv
    email_raw = _get_by_alias(kv, "email")
    first_raw = _get_by_alias(kv, "first_name")
    last_raw = _get_by_alias(kv, "last_name")
    phone_raw = _get_by_alias(kv, "phone")
    movedate_raw = _get_by_alias(kv, "move_date")

    ocity = _get_by_alias(kv, "origin_city")
    ostate = _get_by_alias(kv, "origin_state")
    ozip = _get_by_alias(kv, "origin_zip")

    dcity = _get_by_alias(kv, "dest_city")
    dstate = _get_by_alias(kv, "dest_state")
    dzip = _get_by_alias(kv, "dest_zip")

    bedrooms = _get_by_alias(kv, "bedrooms")
    home_type = _get_by_alias(kv, "home_type")

    # Clean/normalize
    email = _first_email(email_raw) or _first_email(t)
    firstname = (first_raw or "").strip()
    lastname = (last_raw or "").strip()
    phone1 = _digits_only(phone_raw)

    movedte = _parse_move_date(movedate_raw)

    ostate2 = _normalize_state(ostate)
    dstate2 = _normalize_state(dstate)

    servtype = _servtypeid(ostate2, dstate2)

    # movesize / notes
    bedrooms_digits = _digits_only(bedrooms)
    movesize = (f"{bedrooms_digits} bedroom" if bedrooms_digits else "").strip()

    notes_lines = []
    if home_type:
        notes_lines.append(f"Home type: {home_type}")
    notes_lines.append("---- RAW EMAIL TEXT ----")
    notes_lines.append(t)
    notes_str = "\n".join(notes_lines)

    return {
        "servtypeid": servtype,
        "firstname": firstname,
        "lastname": lastname,
        "email": email,
        "phone1": phone1,
        "movedte": movedte,  # REQUIRED by Granot
        "ocity": ocity,
        "ostate": ostate2,
        "ozip": _digits_only(ozip)[:5],
        "dcity": dcity,
        "dstate": dstate2,
        "dzip": _digits_only(dzip)[:5],
        "movesize": movesize[:20],
        "label": "Bronze Star Inbound Email",
        "notes": notes_str,
        "consent": "1",
    }


async def _read_request_any(req: Request) -> Dict[str, Any]:
    """
    Robustly accept:
    - application/json
    - multipart/form-data / x-www-form-urlencoded
    - raw text
    Returns a dict with at least 'text' when possible.
    """
    raw = await req.body()
    raw_text = raw.decode("utf-8", errors="ignore").strip()

    data: Dict[str, Any] = {}

    # 1) Try JSON
    if raw_text:
        try:
            data = json.loads(raw_text)
        except Exception:
            data = {}

    # 2) Try form data
    if not data:
        try:
            form = await req.form()
            data = dict(form)
        except Exception:
            data = {}

    # 3) If still nothing, treat raw body as text itself
    if not data and raw_text:
        data = {"text": raw_text}

    # Helpful debug extras
    data["_raw_preview"] = raw_text[:400]
    data["_content_type"] = req.headers.get("content-type", "")

    return data


# ---------------------------
# Routes
# ---------------------------

@app.get("/health")
def health():
    return {"ok": True}


@app.post("/inbound/bronze-email")
async def inbound_bronze_email(req: Request):
    data = await _read_request_any(req)

    # pull api_id from payload or env
    api_id = (data.get("api_id") or os.getenv("GRANOT_API_ID", "")).strip()
    if not api_id:
        raise HTTPException(status_code=400, detail="Missing api_id or env var GRANOT_API_ID.")

    text = (data.get("text") or "").strip()
    if not text:
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "No 'text' found in request. Send JSON {'text':'...'} or form field text=...",
                "content_type": data.get("_content_type", ""),
                "raw_preview": data.get("_raw_preview", ""),
                "received_keys": sorted([k for k in data.keys() if not k.startswith("_")]),
            },
        )

    lead = parse_lead_from_text(text)

    # Granot requires move date
    if not lead.get("movedte"):
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "Missing/invalid move date after parsing. Expecting Move date like 23/12/2025.",
                "parsed": lead,
            },
        )

    # Basic sanity checks to avoid silent Granot discards
    if not lead.get("phone1") and not lead.get("email"):
        return JSONResponse(
            status_code=422,
            content={"ok": False, "error": "Missing both phone and email after parsing.", "parsed": lead},
        )

    url = f"{GRANOT_BASE_URL}?API_ID={api_id}"

    async with httpx.AsyncClient(timeout=25) as client:
        # Granot expects form post
        resp = await client.post(url, data=lead)

    return {
        "ok": resp.status_code == 200,
        "granot_status": resp.status_code,
        "granot_response": resp.text,
        "posted": lead,
        "debug": {
            "content_type": data.get("_content_type", ""),
            "raw_preview": data.get("_raw_preview", ""),
        },
    }
