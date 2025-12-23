"""
main.py — Bronze Star inbound email parser -> Granot lead post

Endpoints:
- GET  /health                       -> {"ok": true}
- POST /inbound/bronze-email         -> accepts JSON OR form/multipart (e.g., SendGrid Inbound Parse)
                                      expects a "text" field (preferred) or "html" field.

Env vars:
- GRANOT_API_ID            (required) e.g., EA2890F15A60
- GRANOT_MOVERREF          (recommended) mover software key (ask Granot support if unsure)
- GRANOT_URL               (optional) default: https://www.granot.com/LEADSGWHTTP.lidgw
- GRANOT_SERVTYPEID        (optional) default: 102
- LABEL_DEFAULT            (optional) default: Bronze Star Inbound Email
- CONSENT_DEFAULT          (optional) default: 1
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()


# -----------------------------
# helpers
# -----------------------------

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) and v.strip() else default


def _pick_first_nonempty(*vals: Optional[str]) -> str:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return ""


def _norm_whitespace(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # collapse excessive blank lines
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _extract(text: str, label: str) -> str:
    """
    Extract 'Label: value' where label is case-insensitive.
    Returns first match, stripped.
    """
    # Example: "Origin city: Akron"
    pattern = rf"(?im)^\s*{re.escape(label)}\s*:\s*(.+?)\s*$"
    m = re.search(pattern, text)
    return m.group(1).strip() if m else ""


def _extract_any(text: str, labels: Tuple[str, ...]) -> str:
    for lab in labels:
        v = _extract(text, lab)
        if v:
            return v
    return ""


def _extract_email(text: str) -> str:
    # Prefer explicit "Email:" line
    v = _extract_any(text, ("Email", "E-mail"))
    if v:
        # strip any mailto decorations accidentally carried into plain text
        v = re.sub(r"<mailto:.*?>", "", v, flags=re.I).strip()
        # if the value somehow includes extra words, keep first token that looks like email
        m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", v, flags=re.I)
        return m.group(1) if m else v

    # fallback: first email-like in the body
    m = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", text, flags=re.I)
    return m.group(1) if m else ""


def _extract_phone(text: str) -> str:
    v = _extract_any(text, ("Phone", "Phone number", "Telephone"))
    if not v:
        return ""
    digits = re.sub(r"\D", "", v)
    # keep US 10-digit if present; otherwise keep what we got
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) >= 10 else digits


def _parse_move_date(text: str) -> str:
    """
    Input examples:
      23/12/2025  (DD/MM/YYYY)  <- your source
      12/23/2025  (MM/DD/YYYY)
      2025-12-23
    Output: MM/DD/YYYY (Granot)
    """
    raw = _extract_any(text, ("Move date", "Move Date", "Moving date", "Moving Date", "Pickup date", "Pickup Date"))
    raw = raw.strip()
    if not raw:
        return ""

    raw = raw.replace(".", "/").replace("-", "/")
    raw = re.sub(r"\s+", "", raw)

    # Try DD/MM/YYYY first (because your feed uses that)
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except Exception:
            pass

    # last resort: find numbers like 23/12/2025 in the whole text
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        d1, d2, yy = m.group(1), m.group(2), m.group(3)
        # assume DD/MM if first > 12
        dd, mm = (d1, d2) if int(d1) > 12 else (d2, d1)
        try:
            dt = datetime(int(yy), int(mm), int(dd))
            return dt.strftime("%m/%d/%Y")
        except Exception:
            return ""

    return ""


def _movesize_from_bedrooms(text: str) -> str:
    b = _extract_any(text, ("Number of bedrooms", "Bedrooms", "Bed rooms"))
    b = b.strip()
    if not b:
        return ""
    m = re.search(r"(\d+)", b)
    if not m:
        return ""
    n = int(m.group(1))
    if n <= 0:
        return ""
    return f"{n} bedroom"


def _truncate(s: str, max_len: int) -> str:
    s = s.strip()
    return s if len(s) <= max_len else s[:max_len]


async def _get_payload(req: Request) -> Dict[str, Any]:
    """
    Accept JSON, form-data, or multipart.
    Returns dict.
    """
    # Try JSON first
    try:
        return await req.json()
    except Exception:
        pass

    # Then try form
    try:
        form = await req.form()
        return dict(form)
    except Exception:
        pass

    # Nothing parseable
    raw = (await req.body()) or b""
    return {"_raw": raw.decode("utf-8", errors="replace")}


def _build_granot_fields(text: str) -> Dict[str, str]:
    text = _norm_whitespace(text)

    firstname = _extract_any(text, ("First name", "First Name"))
    lastname = _extract_any(text, ("Last name", "Last Name"))
    email = _extract_email(text)
    phone = _extract_phone(text)

    movedte = _parse_move_date(text)

    ocity = _extract_any(text, ("Origin city", "Origin City"))
    ostate = _extract_any(text, ("Origin state", "Origin State"))
    ozip = _extract_any(text, ("Origin zip", "Origin Zip", "Origin zipcode", "Origin Postal Code"))

    dcity = _extract_any(text, ("Moving city", "Moving City", "Destination city", "Destination City"))
    dstate = _extract_any(text, ("Moving state", "Moving State", "Destination state", "Destination State"))
    dzip = _extract_any(text, ("Moving zip", "Moving Zip", "Destination zip", "Destination Zip", "Destination zipcode"))

    movesize = _movesize_from_bedrooms(text)

    home_type = _extract_any(text, ("Home type", "Home Type"))

    notes_parts = []
    if home_type:
        notes_parts.append(f"Home type: {home_type}")
    notes_parts.append("---- RAW EMAIL TEXT ----\n" + text)

    return {
        "firstname": firstname,
        "lastname": lastname,
        "email": email,
        "phone1": phone,
        "movedte": movedte,
        "ocity": ocity,
        "ostate": ostate,
        "ozip": ozip,
        "dcity": dcity,
        "dstate": dstate,
        "dzip": dzip,
        "movesize": movesize,
        "notes": "\n".join(notes_parts).strip(),
    }


def _parse_granot_response(resp_text: str) -> Dict[str, Any]:
    """
    Typical response: leadid,errid,msg,sold,match
    Example from docs: 104360,0,,0,0
    You are getting: 0,0,,0,0  (leadid=0 => usually means it did not actually insert)
    """
    raw = resp_text.strip()
    parts = [p.strip() for p in raw.split(",")]
    # Pad to 5
    while len(parts) < 5:
        parts.append("")
    leadid, errid, msg, sold, match = parts[:5]
    out = {
        "leadid": leadid,
        "errid": errid,
        "msg": msg,
        "sold": sold,
        "match": match,
        "raw": raw,
    }
    try:
        out["leadid_int"] = int(leadid) if leadid else 0
    except Exception:
        out["leadid_int"] = 0
    try:
        out["errid_int"] = int(errid) if errid else -1
    except Exception:
        out["errid_int"] = -1
    return out


# -----------------------------
# routes
# -----------------------------

@app.get("/health")
def health():
    return {"ok": True}


@app.post("/inbound/bronze-email")
async def inbound_bronze_email(req: Request):
    payload = await _get_payload(req)

    # The inbound webhook from GHL sends JSON, with the entire email body in payload["text"].
    # SendGrid inbound parse will post "text" and/or "html" in multipart form.
    raw_text = _pick_first_nonempty(
        str(payload.get("text") or ""),
        str(payload.get("Text") or ""),
        str(payload.get("html") or ""),
        str(payload.get("Html") or ""),
        str(payload.get("_raw") or ""),
    )

    if not raw_text.strip():
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Missing text/html in request payload.", "payload_keys": sorted(payload.keys())},
        )

    fields = _build_granot_fields(raw_text)

    # Validate minimal differentiation — must have at least email or phone for downstream GHL "Create Contact"
    if not (fields.get("email") or fields.get("phone1")):
        return JSONResponse(
            status_code=422,
            content={"ok": False, "error": "Missing email and phone after parsing.", "parsed": fields},
        )

    # Granot requires a move date; if missing, fail loudly (your earlier error)
    if not fields.get("movedte"):
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "Missing/invalid move date after parsing. Expecting Move date like 23/12/2025.",
                "parsed": fields,
            },
        )

    api_id = _env("GRANOT_API_ID")
    if not api_id:
        return JSONResponse(status_code=500, content={"ok": False, "error": "GRANOT_API_ID env var missing."})

    moverref = _env("GRANOT_MOVERREF")  # strongly recommended
    base_url = _env("GRANOT_URL", "https://www.granot.com/LEADSGWHTTP.lidgw")
    servtypeid = _env("GRANOT_SERVTYPEID", "102")
    label = _truncate(_env("LABEL_DEFAULT", "Bronze Star Inbound Email"), 20)
    consent = _env("CONSENT_DEFAULT", "1")

    post_fields = {
        "servtypeid": servtypeid,
        "firstname": fields["firstname"],
        "lastname": fields["lastname"],
        "email": fields["email"],
        "phone1": fields["phone1"],
        "movedte": fields["movedte"],
        "ocity": fields["ocity"],
        "ostate": fields["ostate"],
        "ozip": fields["ozip"],
        "dcity": fields["dcity"],
        "dstate": fields["dstate"],
        "dzip": fields["dzip"],
        "movesize": fields["movesize"],
        "label": label,
        "notes": fields["notes"],
        "consent": consent,
    }

    # Build URL: docs indicate ONLY API_ID and MOVERREF should be in query for direct posting into mover software.
    # If you omit MOVERREF, Granot may accept the post but not insert it into your mover's lead table (leadid=0).
    url = f"{base_url}?API_ID={api_id}"
    if moverref:
        url += f"&MOVERREF={moverref}"

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(url, data=post_fields)
        granot_status = r.status_code
        granot_text = (r.text or "").strip()

    parsed_resp = _parse_granot_response(granot_text)

    # Consider it "success" only if errid==0 AND leadid>0
    ok = (parsed_resp.get("errid_int") == 0) and (parsed_resp.get("leadid_int", 0) > 0)

    # If errid==0 but leadid==0, return ok=false with a clear hint: MOVERREF is likely missing/wrong.
    if parsed_resp.get("errid_int") == 0 and parsed_resp.get("leadid_int", 0) == 0:
        return JSONResponse(
            status_code=200,
            content={
                "ok": False,
                "warning": "Granot returned errid=0 but leadid=0. This usually means the post was accepted but NOT inserted into your mover's system. Verify GRANOT_MOVERREF (lead email key) with Granot support.",
                "granot_status": granot_status,
                "granot_response": parsed_resp,
                "posted": post_fields,
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "ok": ok,
            "granot_status": granot_status,
            "granot_response": parsed_resp,
            "posted": post_fields,
        },
    )


@app.post("/bronze-email")
async def bronze_email_alias(req: Request):
    return await inbound_bronze_email(req)
