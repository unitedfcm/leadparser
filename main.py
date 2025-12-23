"""
main.py — Bronze Star inbound email parser -> Granot direct post (FINAL)

Granot doc requirements implemented:
- Direct post URL must include ONLY API_ID and MOVERREF. (no other field names in URL)
  https://lead.hellomoving.com/LEADSGWHTTP.lidgw?&API_ID=XXXX&MOVERREF=leads@company.com
- movedte is mandatory and must be MM/DD/YYYY
- Granot expects HTTP form POST fields

Endpoints:
- GET  /health
- POST /inbound/bronze-email
"""

import os
import re
import json
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

GRANOT_BASE_URL = "https://lead.hellomoving.com/LEADSGWHTTP.lidgw"

EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)


def _clean_text(s: str) -> str:
    if not s:
        return ""

    # If GHL sends literal escaped sequences inside JSON strings, normalize them:
    # "\\r\\n" -> "\n", etc.
    if "\\r\\n" in s or "\\n" in s or "\\r" in s:
        s = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")

    # Normalize actual CRLF too
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # Remove common html-ish artifacts from inbound email bodies
    s = s.replace("&nbsp;", " ")
    s = re.sub(r"<mailto:.*?>", "", s, flags=re.IGNORECASE)

    # Collapse whitespace
    s = re.sub(r"[ \t]+", " ", s)

    # Trim and keep stable newlines
    s = "\n".join([ln.rstrip() for ln in s.split("\n")]).strip()

    return s


def _digits_only(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalize_state(s: str) -> str:
    s = (s or "").strip().upper()
    m = re.search(r"\b([A-Z]{2})\b", s)
    return m.group(1) if m else (s[:2] if len(s) >= 2 else "")


def _first_email(text: str) -> str:
    m = EMAIL_RE.search(text or "")
    return m.group(1).strip() if m else ""


def _parse_move_date_to_mmddyyyy(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""

    # Your emails show DD/MM/YYYY (23/12/2025). Convert to MM/DD/YYYY.
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return ""


def _servtypeid(ostate: str, dstate: str) -> str:
    # 101 local (same state) else 102 long distance
    if ostate and dstate and ostate.upper() == dstate.upper():
        return "101"
    return "102"


def _kv_from_lines(text: str) -> Dict[str, str]:
    """
    Parses lines like:
      "Email: x"
      "First name: y"
    into {"email":"x", "first name":"y", ...}
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
        if k and k not in kv:
            kv[k] = v
    return kv


def _get(kv: Dict[str, str], *keys: str) -> str:
    for k in keys:
        kk = k.strip().lower()
        if kk in kv and kv[kk]:
            return kv[kk].strip()
    return ""


def _parse_bedrooms_to_movesize(raw: str) -> str:
    n = _digits_only(raw)
    if not n:
        return ""
    # Keep short (doc says 20 chars for movesize)
    return f"{n} bedroom"[:20]


def parse_lead_from_email_text(text: str) -> Dict[str, str]:
    t = _clean_text(text)
    kv = _kv_from_lines(t)

    email_raw = _get(kv, "email")
    firstname = _get(kv, "first name", "firstname")
    lastname = _get(kv, "last name", "lastname")
    phone_raw = _get(kv, "phone", "phone number")
    move_date_raw = _get(kv, "move date", "moving date")

    ocity = _get(kv, "origin city", "from city", "pickup city")
    ostate = _normalize_state(_get(kv, "origin state", "from state", "pickup state"))
    ozip = _digits_only(_get(kv, "origin zip", "from zip", "origin zipcode"))[:5]

    dcity = _get(kv, "moving city", "destination city", "to city", "dropoff city")
    dstate = _normalize_state(_get(kv, "moving state", "destination state", "to state", "dropoff state"))
    dzip = _digits_only(_get(kv, "moving zip", "destination zip", "to zip", "destination zipcode"))[:5]

    bedrooms_raw = _get(kv, "number of bedrooms", "bedrooms")
    home_type = _get(kv, "home type")

    # Fallback extraction (in case the "Email:" line is weird)
    email = _first_email(email_raw) or _first_email(t)

    phone1 = _digits_only(phone_raw)
    movedte = _parse_move_date_to_mmddyyyy(move_date_raw)
    servtypeid = _servtypeid(ostate, dstate)
    movesize = _parse_bedrooms_to_movesize(bedrooms_raw)

    notes_lines = []
    if home_type:
        notes_lines.append(f"Home type: {home_type}")
    notes_lines.append("---- RAW EMAIL TEXT ----")
    notes_lines.append(t)
    notes = "\n".join(notes_lines)

    return {
        "servtypeid": servtypeid,
        "firstname": firstname[:30],
        "lastname": lastname[:30],
        "email": email[:50],
        "phone1": phone1[:20],
        "movedte": movedte,            # REQUIRED (MM/DD/YYYY)
        "ocity": ocity[:30],
        "ostate": ostate[:20],
        "ozip": ozip[:6],
        "dcity": dcity[:20],
        "dstate": dstate[:20],
        "dzip": dzip[:6],
        "movesize": movesize[:20],
        "label": "MAX EXCLUSIVE"[:20],
        "notes": notes,
        "consent": "1",
    }


async def _read_request_any(req: Request) -> Dict[str, Any]:
    """
    Accepts:
    - application/json
    - multipart/form-data / x-www-form-urlencoded
    - raw text fallback
    """
    raw = await req.body()
    raw_text = raw.decode("utf-8", errors="ignore")

    data: Dict[str, Any] = {}

    # Try JSON first if it looks like JSON
    try:
        data = json.loads(raw_text) if raw_text.strip() else {}
    except Exception:
        data = {}

    # Try form-data
    if not data:
        try:
            form = await req.form()
            data = dict(form)
        except Exception:
            data = {}

    # Fallback: treat whole body as the text
    if not data and raw_text.strip():
        data = {"text": raw_text}

    data["_raw_preview"] = (raw_text or "")[:500]
    data["_content_type"] = req.headers.get("content-type", "")
    return data


def _parse_granot_response(resp_text: str) -> Dict[str, str]:
    """
    Granot HTTP response example: "104360,0,OK,6,6"
    Format: leadid,errid,msg,sold,match
    """
    t = (resp_text or "").strip()
    parts = [p.strip() for p in t.split(",")]
    out = {"leadid": "", "errid": "", "msg": "", "sold": "", "match": "", "raw": t}

    if len(parts) >= 5:
        out["leadid"] = parts[0]
        out["errid"] = parts[1]
        out["msg"] = parts[2]
        out["sold"] = parts[3]
        out["match"] = parts[4]
    return out


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/inbound/bronze-email")
async def inbound_bronze_email(req: Request):
    data = await _read_request_any(req)

    api_id = (data.get("api_id") or os.getenv("GRANOT_API_ID", "")).strip()
    if not api_id:
        raise HTTPException(status_code=400, detail="Missing GRANOT_API_ID (or api_id in payload).")

    moverref = (data.get("moverref") or os.getenv("GRANOT_MOVERREF", "")).strip()
    if not moverref:
        # If you want direct-to-mover posting, this is required.
        # Granot can also accept marketplace posting without moverref,
        # but your situation needs direct posting.
        raise HTTPException(status_code=400, detail="Missing GRANOT_MOVERREF (or moverref in payload).")

    text = (data.get("text") or "")
    if not str(text).strip():
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "No 'text' found in request. Send JSON {'text':'...'}",
                "debug": {
                    "content_type": data.get("_content_type", ""),
                    "raw_preview": data.get("_raw_preview", ""),
                    "received_keys": sorted([k for k in data.keys() if not k.startswith("_")]),
                },
            },
        )

    lead = parse_lead_from_email_text(str(text))

    # Hard requirements per Granot docs:
    if not lead.get("movedte"):
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "Missing/invalid move date after parsing. Expecting Move date like 23/12/2025.",
                "parsed": lead,
            },
        )

    # Must have at least one contact method (Granot error 15 if missing both) — doc shows phone/email recommended.
    if not lead.get("phone1") and not lead.get("email"):
        return JSONResponse(
            status_code=422,
            content={"ok": False, "error": "Missing both phone and email after parsing.", "parsed": lead},
        )

    # Ensure states are present because Granot can error on missing from/to state (err 17/18)
    if not lead.get("ostate") or not lead.get("dstate"):
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "Missing origin/destination state after parsing (Origin state / Moving state).",
                "parsed": lead,
            },
        )

    # Doc: direct post URL must contain ONLY API_ID + MOVERREF
    url = f"{GRANOT_BASE_URL}?&API_ID={api_id}&MOVERREF={moverref}"

    # Doc: moverref is optional in POST body IF provided on URL.
    # We'll include it anyway; it does not violate the "URL only" rule.
    lead_for_post = dict(lead)
    lead_for_post["moverref"] = moverref

    async with httpx.AsyncClient(timeout=25) as client:
        # Granot expects form post
        resp = await client.post(url, data=lead_for_post)

    granot_parsed = _parse_granot_response(resp.text)

    return {
        "ok": resp.status_code == 200 and (granot_parsed.get("errid") in ("", "0")),
        "granot_status": resp.status_code,
        "granot_parsed": granot_parsed,
        "posted": lead_for_post,
        "debug": {
            "content_type": data.get("_content_type", ""),
            "raw_preview": data.get("_raw_preview", ""),
            "post_url_has_only": "API_ID + MOVERREF",
        },
    }
