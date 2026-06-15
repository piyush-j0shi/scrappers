"""
Scraper monitor API — FastAPI endpoint that receives branch completion reports
from all scrapers (New World, PAK'nSAVE, Woolworths).

Run with:
    uvicorn scraper_api:app --host 0.0.0.0 --port 8765 --reload

Reports are held in memory for the lifetime of the process.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Scraper Monitor", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pydantic models ────────────────────────────────────────────────────────────

class CategoryResult(BaseModel):
    name: str
    status: str          # "success" | "empty" | "failed"
    products: int
    reason: Optional[str] = None


class BranchReport(BaseModel):
    chain: str
    branch_name: str
    branch_id: Optional[str] = None
    store_id: Optional[str] = None
    status: str          # "success" | "partial" | "failed"
    total_products: int
    categories: list[CategoryResult]
    price_changes: int
    specials: int
    out_of_stock: int
    timestamp: datetime


class ReportAck(BaseModel):
    ok: bool
    message: str


class Summary(BaseModel):
    total_reports: int
    chains: list[str]
    last_received: Optional[datetime]


# ── In-memory store ────────────────────────────────────────────────────────────

_reports: list[BranchReport] = []


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.post("/branch-complete", response_model=ReportAck)
async def branch_complete(report: BranchReport) -> ReportAck:
    _reports.append(report)
    failed_cats = [c for c in report.categories if c.status == "failed"]
    print(
        f"[{report.chain}] {report.branch_name} — {report.status} — "
        f"{report.total_products} products  specials={report.specials}  "
        f"out_of_stock={report.out_of_stock}  price_changes={report.price_changes}  "
        f"cat_failures={len(failed_cats)}"
    )
    return ReportAck(ok=True, message="received")


@app.get("/reports", response_model=list[BranchReport])
async def get_reports() -> list[BranchReport]:
    return _reports


@app.get("/reports/{chain}", response_model=list[BranchReport])
async def get_chain_reports(chain: str) -> list[BranchReport]:
    matches = [r for r in _reports if r.chain.lower() == chain.lower()]
    if not matches:
        raise HTTPException(status_code=404, detail=f"No reports for chain '{chain}'")
    return matches


@app.get("/summary", response_model=Summary)
async def get_summary() -> Summary:
    chains = sorted({r.chain for r in _reports})
    last = _reports[-1].timestamp if _reports else None
    return Summary(total_reports=len(_reports), chains=chains, last_received=last)


@app.delete("/reports")
async def clear_reports() -> ReportAck:
    _reports.clear()
    return ReportAck(ok=True, message="cleared")
