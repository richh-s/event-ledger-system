"""
src/agents/registry_client.py
=============================
Read-only client for the Applicant Registry (CRM).
Used by agents to load historical context.
"""
from __future__ import annotations
from dataclasses import dataclass, fields
from typing import List, Optional, Dict, Any
import asyncpg
from decimal import Decimal

@dataclass
class CompanyProfile:
    company_id: str
    name: str
    industry: str
    naics: str
    jurisdiction: str
    legal_type: str
    founded_year: int
    employee_count: int
    ein: str
    address_city: str
    address_state: str
    risk_segment: str
    trajectory: str
    submission_channel: str
    ip_region: str

@dataclass
class FinancialYear:
    fiscal_year: int
    total_revenue: float
    gross_profit: float
    operating_income: float
    ebitda: float
    net_income: float
    total_assets: float
    total_liabilities: float
    total_equity: float
    long_term_debt: float
    cash_and_equivalents: float
    current_assets: float
    current_liabilities: float
    accounts_receivable: float
    inventory: float
    debt_to_equity: float
    current_ratio: float
    debt_to_ebitda: float
    interest_coverage_ratio: float
    gross_margin: float
    ebitda_margin: float
    net_margin: float

@dataclass
class ComplianceFlag:
    flag_type: str
    severity: str
    is_active: bool
    added_date: str
    note: str

class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    """
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_company(self, company_id: str) -> Optional[CompanyProfile]:
        query = "SELECT * FROM applicant_registry.companies WHERE company_id = $1"
        row = await self._pool.fetchrow(query, company_id)
        if not row:
            return None
        # Robust filtering for extra DB columns
        valid_fields = {f.name for f in fields(CompanyProfile)}
        data = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in dict(row).items() if k in valid_fields}
        return CompanyProfile(**data)

    async def get_financial_history(self, company_id: str, years: Optional[List[int]] = None) -> List[FinancialYear]:
        if years:
            query = "SELECT * FROM applicant_registry.financial_history WHERE company_id = $1 AND fiscal_year = ANY($2) ORDER BY fiscal_year ASC"
            rows = await self._pool.fetch(query, company_id, years)
        else:
            query = "SELECT * FROM applicant_registry.financial_history WHERE company_id = $1 ORDER BY fiscal_year ASC"
            rows = await self._pool.fetch(query, company_id)
        
        from dataclasses import fields
        valid_fields = {f.name for f in fields(FinancialYear)}
        results = []
        for row in rows:
            data = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in dict(row).items() if k in valid_fields}
            results.append(FinancialYear(**data))
        return results

    async def get_compliance_flags(self, company_id: str, active_only: bool = False) -> List[ComplianceFlag]:
        if active_only:
            query = "SELECT * FROM applicant_registry.compliance_flags WHERE company_id = $1 AND is_active = TRUE"
        else:
            query = "SELECT * FROM applicant_registry.compliance_flags WHERE company_id = $1"
        rows = await self._pool.fetch(query, company_id)
        
        from dataclasses import fields
        valid_fields = {f.name for f in fields(ComplianceFlag)}
        results = []
        for row in rows:
            data = {k: (float(v) if isinstance(v, Decimal) else v) for k, v in dict(row).items() if k in valid_fields}
            results.append(ComplianceFlag(**data))
        return results

    async def get_loan_relationships(self, company_id: str) -> List[Dict[str, Any]]:
        query = "SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1"
        rows = await self._pool.fetch(query, company_id)
        return [dict(row) for row in rows]
