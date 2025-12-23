import os
import re
from datetime import datetime
from typing import Optional, Dict, Any

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI()

GRANOT_BASE_URL = "https://lead.hellomoving.com/LEADSGWHTTP.lidgw"

def _clean_text(s: str) -> str:
    if not s:
        return ""
    # Remove HTML-ish artifacts like &nbsp; and <mailto:...>
    s = s.replace("&nbsp;", " ")
    s = re.sub(r"<mailto:.*?>", "", s, flags=re.IGNORECASE)
    return s.strip()

def _extract(pattern: str, text: str) -> str:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    return m.group(1).strip() if m else ""

def _parse_move_date(raw: str) -> str:
    """
    Incoming is often DD/MM/YYYY (like 23/12/2025).
    Granot requires MM/DD/YYYY.
    """
    raw = raw.strip()
    if not raw:
        return ""

    # Try DD/MM/YYYY
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return ""  # will fail validation at Granot if required

def _servtypeid(ostate: str, dstate: str) -> str:
    if ostate and dstate and ostate.upper() == dstate.upper():
        return "101"  # local
    return "102"      # long distance / default

def parse_lead_from_text(text: str) -> Dict[str, str]:
    t = _clean_text(text)

    email = _extract(r"Email:\s*([^\s<]+)", t)
    firstname = _extract(r"First name:\s*([^\r\n]+)", t)
    lastname = _extract(r"Last name:\s*([^\r\n]+)", t)
    phone = _extract(r"Phone:\s*([0-9\-\(\)\s\+]+)", t)
    movedate_raw = _extract(r"Move date:\s*([0-9\/\-]+)", t)
    ocity = _extract(r"Origin city:\s*([^\r\n]+)", t)
    ostate = _extract(r"Origin state:\s*([A-Za-z]{2})", t)
    ozip = _extract(r"Origin zip:\s*([0-9]{5})", t)
    dcity = _extract(r"Moving city:\s*([^\r\n]+)", t)
    dstate = _extract(r"Moving state:\s*([A-Za-z]{2})", t)
    dzip = _extract(r"Moving zip:\s*([0-9]{5})", t)
    bedrooms = _extract(r"Number of bedrooms:\s*([0-9]+)", t)
    home_type = _extract(r"Home type:\s*([^\r\n]+)", t)

    movedte = _parse_move_date(movedate_raw)
    servtype = _servtypeid(ostate, dstate)

    movesize = (f"{bedrooms} bedroom" if bedrooms else "").strip()
    notes = []
    if home_type:
        notes.append(f"Home type: {home_type}")
    # Keep the raw text in notes too for audit/debug
    notes.append("---- RAW EMAIL TEXT ----")
    notes.append(t)
    notes_str = "\n".join(notes)

    return {
        "servtypeid": servtype,
        "firstname": firstname,
        "lastname": lastname,
        "email": email,
        "phone1": re.sub(r"\D", "", phone),  # digits only
        "movedte": movedte,
        "ocity": ocity,
        "ostate": ostate.upper(),
        "ozip": ozip,
        "dcity": dcity,
        "dstate": dstate.upper(),
        "dzip": dzip,
        "movesize": movesize[:20],          # Granot field is short
        "label": "Bronze Star Inbound Email",
        "notes": notes_str,
        "consent": "1",
    }

@app.post("/inbound/bronze-email")
async def inbound_bronze_email(req: Request):
    data: Dict[str, Any] = await req.json()
    api_id = data.get("api_id") or os.getenv("GRANOT_API_ID", "").strip()
    text = data.get("text", "")

    if not api_id:
        raise HTTPException(status_code=400, detail="Missing api_id (Granot API_ID).")

    lead = parse_lead_from_text(text)

    # Granot requires movedte
    if not lead.get("movedte"):
        return JSONResponse(
            status_code=422,
            content={
                "ok": False,
                "error": "Missing/invalid move date after parsing. Granot requires movedte in MM/DD/YYYY.",
                "parsed": lead,
            },
        )

    url = f"{GRANOT_BASE_URL}?API_ID={api_id}"

    # Granot supports a simple form post
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, data=lead)

    # Return both the lead we posted and Granot's raw response text
    return {
        "ok": resp.status_code == 200,
        "granot_status": resp.status_code,
        "granot_response": resp.text,
        "posted": lead,
    }

@app.get("/health")
def health():
    return {"ok": True}
