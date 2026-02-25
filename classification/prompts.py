"""Prompt templates for Claude API classification and extraction."""

from classification.doc_types import DOC_TYPE_DESCRIPTIONS

CLASSIFICATION_SYSTEM_PROMPT = """You are a KYC (Know Your Customer) document classification specialist for a financial institution. Your task is to:
1. Identify the type of KYC document
2. Identify which company (counterparty) the document belongs to

You must respond with valid JSON only, no other text."""

CLASSIFICATION_USER_PROMPT = f"""Analyze the following document and classify it.

## Document Types
{DOC_TYPE_DESCRIPTIONS}

## Documents That Are NOT KYC Documents
The following types of documents do NOT belong to any KYC category above and must be classified as "unknown":
- Company profiles, company introductions, or corporate brochures (公司简介/宣传册)
- Product catalogues or service brochures
- ISO certificates, quality management certificates, or other industry certifications (e.g. ISO 9001, ISO 14001)
- Marketing materials, advertisements, or promotional flyers
- Internal memos, general correspondence, or cover letters
- Press releases, news articles, or media clippings
- Employee handbooks, HR policies, or training materials
- General contracts, purchase orders, or invoices (unless they serve as source-of-funds evidence)

## Instructions
1. Determine ALL applicable document types from the list above. A single document may satisfy multiple KYC requirements.
   - For example, Articles of Incorporation may also contain M&A (公司章程) content like general provisions, shareholder meeting rules, etc.
   - An ownership structure chart may also serve as a shareholders & directors list if it contains shareholder names and percentages.
2. List the primary document type first, then any additional types the document also covers.
3. Extract the company name this document belongs to.
4. Assess your confidence level (0.0 to 1.0).
5. Confidence calibration rules:
   - If the document clearly does NOT match any KYC document type (e.g. company brochure, ISO certificate, marketing material), you MUST classify it as ["unknown"] with confidence <= 0.3.
   - Only use high confidence (0.8+) when the document clearly and unambiguously matches one or more KYC document types based on the key features described above.
   - For borderline cases, use moderate confidence (0.4-0.7) and explain your uncertainty in the reasoning.

## Response Format (JSON only)
{{
    "doc_types": ["<primary_type>", "<additional_type1>", ...],
    "company_name": "<full company name as it appears in the document>",
    "confidence": <0.0-1.0>,
    "reasoning": "<brief explanation of classification, including why additional types apply>"
}}

If the document only matches one type, return a single-element array: ["<type>"].
If you cannot determine the document type, use ["unknown"].
If you cannot determine the company name, use "unknown".

{{few_shot_block}}## Document Content:
{{content}}"""

# Per-document-type extraction prompts
EXTRACTION_PROMPTS = {
    "bizfile": """Extract the following from this company registration (Bizfile) document:
{
    "company_name": "full registered name",
    "registration_number": "UEN / registration number",
    "incorporation_date": "YYYY-MM-DD",
    "company_type": "e.g. Private Limited, Public Limited",
    "registered_address": "full address",
    "company_status": "e.g. Live, Struck Off",
    "principal_activities": ["list of business activities"],
    "paid_up_capital": "amount with currency"
}
Return valid JSON only.""",

    "incorporation": """Extract the following from these incorporation documents:
{
    "company_name": "full registered name",
    "registration_number": "registration/incorporation number",
    "incorporation_date": "YYYY-MM-DD",
    "jurisdiction": "country/state of incorporation",
    "company_type": "entity type",
    "authorized_share_capital": "if mentioned",
    "initial_directors": ["list of director names"],
    "initial_shareholders": ["list of shareholder names"]
}
Return valid JSON only.""",

    "maa": """Extract the following from this Memorandum and Articles of Association:
{
    "company_name": "full company name",
    "date_adopted": "YYYY-MM-DD if available",
    "share_classes": ["types of shares described"],
    "director_appointment_rules": "summary of key rules",
    "key_provisions": ["list of notable provisions"],
    "amendment_date": "last amendment date if visible"
}
Return valid JSON only.""",

    "shareholders_directors": """Extract the following from this shareholders and directors list:
{
    "company_name": "company name",
    "document_date": "YYYY-MM-DD",
    "directors": [
        {"name": "", "nationality": "", "id_number": "", "appointment_date": ""}
    ],
    "shareholders": [
        {"name": "", "shares": 0, "percentage": 0.0, "share_type": ""}
    ],
    "total_shares": 0,
    "secretary": "company secretary name if listed"
}
Return valid JSON only.""",

    "ownership_structure": """Extract the following from this ownership/organizational structure chart:
{
    "company_name": "target company name",
    "ultimate_beneficial_owners": [
        {"name": "", "nationality": "", "percentage": 0.0}
    ],
    "intermediate_entities": [
        {"name": "", "jurisdiction": "", "ownership_percentage": 0.0, "parent": ""}
    ],
    "director_signed": true/false,
    "signing_director_name": "",
    "document_date": "YYYY-MM-DD if available"
}
Return valid JSON only.""",

    "financial_reports": """Extract the following from these financial reports/statements:
{
    "company_name": "company name",
    "report_type": "audited_annual / quarterly / other",
    "fiscal_year_end": "YYYY-MM-DD",
    "period_covered": "e.g. FY2023 or Q3 2024",
    "auditor_name": "audit firm name if audited report",
    "audit_opinion": "unqualified/qualified/adverse/disclaimer",
    "total_revenue": "amount with currency",
    "net_profit_loss": "amount with currency",
    "total_assets": "amount with currency",
    "total_liabilities": "amount with currency",
    "total_equity": "amount with currency",
    "currency": "reporting currency"
}
Return valid JSON only.""",

    "bank_reference": """Extract the following from this bank account reference:
{
    "company_name": "account holder name",
    "bank_name": "bank name",
    "branch": "branch if mentioned",
    "account_number": "partially masked is ok",
    "account_type": "current/savings/etc",
    "currency": "account currency",
    "date_opened": "YYYY-MM-DD if available",
    "reference_date": "date of this letter YYYY-MM-DD",
    "signatory": "bank officer name/title"
}
Return valid JSON only.""",

    "kyc_questionnaire": """Extract the following from this KYC questionnaire:
{
    "company_name": "company name",
    "authorized_signatory": "name of person who signed",
    "signatory_title": "title/position",
    "date_signed": "YYYY-MM-DD",
    "business_nature": "description of business",
    "countries_of_operation": ["list of countries"],
    "expected_transaction_volume": "if mentioned",
    "source_of_funds": "described source",
    "pep_declarations": "any PEP (Politically Exposed Person) declarations",
    "sanctions_screening": "any sanctions-related declarations"
}
Return valid JSON only.""",

    "onboarding_form": """Extract the following from this Counterparty On-boarding Application Form (对家注册申请表):
{
    "counterparty_name_zh": "counterparty Chinese name",
    "counterparty_name_en": "counterparty English name",
    "incorporation_date": "YYYY-MM-DD",
    "business_nature": "企业性质",
    "legal_representative": "法定代表人/执行董事",
    "business_scope": "经营范围",
    "registered_capital": "注册资本 with currency",
    "net_assets": "净资产 with currency",
    "registered_address": "注册地址",
    "contact_person": "对家联络人",
    "contact_email": "联络邮箱",
    "contact_phone": "联络电话",
    "proposed_products": "拟交易产品/业务",
    "transaction_background": "交易背景和理由",
    "business_supervisor": "业务负责人",
    "applicant": "提起人",
    "department": "部门",
    "application_date": "YYYY-MM-DD",
    "urgency": "一般/紧急/特急",
    "vp_signed": true/false,
    "vp_signature_name": "VP signer name if visible",
    "onboarder_review": "注册专员 KYC审查意见",
    "risk_management_review": "风控意见"
}
Return valid JSON only.""",

    "source_of_funds": """Extract the following from this source of funds proof:
{
    "company_name": "company name",
    "fund_sources": ["list of funding sources described"],
    "evidence_type": "bank statement / auditor letter / board resolution / other",
    "total_amount": "if specified, amount with currency",
    "period_covered": "time period the evidence covers",
    "supporting_documents": ["any referenced supporting documents"],
    "certified_by": "name and title if certified"
}
Return valid JSON only.""",
}

EXTRACTION_SYSTEM_PROMPT = """You are a KYC document data extraction specialist. Extract structured information from documents accurately.
- For dates, use YYYY-MM-DD format
- For amounts, include the currency symbol or code
- If a field is not found in the document, use null
- Return valid JSON only, no other text"""
