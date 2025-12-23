"""
main.py — Bronze Star inbound email parser -> Granot poster

What it does:
- Accepts webhook payloads from GHL / Postman / anything.
- Robustly reads request body as:
    1) JSON (application/json)
    2) form-data / x-www-form-urlencoded
    3) raw text fallback
- Extracts fields from the inbound email text (your "New Moving Lead" format)
- Normalizes move date to MM/DD/YYYY (Granot requirement)
- Sets servtypeid (101 if same-state, else 102)
- Posts the structured lead to Granot using API_ID.

Endpoints:
- GET  /health
- POST /inbound/bronze-email
"""

import os
import re
import json
from datetime import datetime
from typing import Dict, Any

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

GRANOT_BASE_URL = "https://lead.hellomoving.com/LEADSGWHTTP.lidgw"


def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("&nbsp;", " ")
    # remove <mailto:...> artifacts
    s = re.sub(r"<mailto:.*?>", "", s, flags=re.IGNORECASE)
    return s.strip()


def _extract(pattern: str, text: str) -> str:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    return m.group(1).strip() if m else ""


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
    return "102"      # long distance / default


def parse_lead_from_text(text: str) -> Dict[str, str]:
    t = _clean_text(text)

    email = _extract(r"Email:\s*([^\s<]+)", t)
    firstname = _extract(r"First name:\s*([^\r\n]+)", t)
    lastname = _extract(r"Last name:\s*([^\r\n]+)", t)
    phone_raw = _extract(r"Phone:\s*([0-9\-\(\)\s\+]+)", t)

    movedate_raw = _extract(r"Move date:\s*([0-9\/\-]+)", t)

    ocity = _extract(r"Origin city:\s*([^\r\n]+)", t)
    ostate = _extract(r"Origin state:\s*([A-Za-z]{2})", t)
    ozip = _extract(r"Origin zip:\s*([0-9]{5})", t)

    dcity = _extract(r"(Moving city|Destination city):\s*([^\r\n]+)", t)
    # if regex above matched with group2 due to (Moving city|Destination city)
    if dcity and re.match(r"^(moving city|destination city)$", dcity.strip(), re.IGNORECASE):
        dcity = ""  # safety (shouldn't happen, but keeps it clean)
    if not dcity:
        dcity = _extract(r"Moving city:\s*([^\r\n]+)", t)

    dstate = _extract(r"(Moving state|Destination state):\s*([A-Za-z]{2})", t)
    if not dstate:
        dstate = _extract(r"Moving state:\s*([A-Za-z]{2})", t)

    dzip = _extract(r"(Moving zip|Destination zip):\s*([0-9]{5})", t)
    if not dzip:
        dzip = _extract(r"Moving zip:\s*([0-9]{5})", t)

    bedrooms = _extract(r"Number of bedrooms:\s*([0-9]+)", t)
    home_type = _extract(r"Home type:\s*([^\r\n]+)", t)

    movedte = _parse_move_date(movedate_raw)
    servtype = _servtypeid(ostate, dstate)

    # Clean phone to digits only
    phone1 = re.sub(r"\D", "", phone_raw or "")

    movesize = (f"{bedrooms} bedroom" if bedrooms else "").strip()

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
        "movedte": movedte,          # REQUIRED by Granot
        "ocity": ocity,
        "ostate": (ostate or "").upper(),
        "ozip": ozip,
        "dcity": dcity,
        "dstate": (dstate or "").upper(),
        "dzip": dzip,
        "movesize": movesize[:20],   # keep short
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
