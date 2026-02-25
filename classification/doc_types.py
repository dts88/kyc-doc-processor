"""KYC document type definitions and mappings."""

from dataclasses import dataclass


@dataclass
class KYCDocType:
    id: int
    code: str
    name_en: str
    name_zh: str
    required: bool
    sort_order: int
    folder_name: str  # for classified storage


# All 10 KYC document types
DOC_TYPES = {
    "bizfile": KYCDocType(1, "bizfile", "Company Registration (Bizfile)", "公司注册资料", True, 1, "01_bizfile"),
    "incorporation": KYCDocType(2, "incorporation", "Incorporation Documents", "公司成立文件", True, 2, "02_incorporation"),
    "maa": KYCDocType(3, "maa", "Memorandum and Articles of Association", "公司章程", True, 3, "03_maa"),
    "shareholders_directors": KYCDocType(4, "shareholders_directors", "Shareholders & Directors List", "股东和董事名单", True, 4, "04_shareholders_directors"),
    "ownership_structure": KYCDocType(5, "ownership_structure", "Ownership/Org Structure Chart", "所有权/组织架构图", True, 5, "05_ownership_structure"),
    "financial_reports": KYCDocType(6, "financial_reports", "Financial Reports", "审计报告和财报", True, 6, "06_financial_reports"),
    "bank_reference": KYCDocType(7, "bank_reference", "Bank Account Reference", "银行账户资料", True, 7, "07_bank_reference"),
    "kyc_questionnaire": KYCDocType(8, "kyc_questionnaire", "KYC Questionnaire", "KYC问卷", True, 8, "08_kyc_questionnaire"),
    "onboarding_form": KYCDocType(9, "onboarding_form", "Counterparty On-boarding Application Form", "对家注册申请表", True, 9, "09_onboarding_form"),
    "source_of_funds": KYCDocType(10, "source_of_funds", "Source of Funds Proof", "资金来源证明", False, 10, "10_source_of_funds"),
    "others": KYCDocType(11, "others", "Others", "其他文件", False, 99, "99_others"),
}

# Required document type codes
REQUIRED_DOC_CODES = [code for code, dt in DOC_TYPES.items() if dt.required]

# Map from ID to code
ID_TO_CODE = {dt.id: code for code, dt in DOC_TYPES.items()}
CODE_TO_ID = {code: dt.id for code, dt in DOC_TYPES.items()}

# Folder for files that belong to a company but don't match any KYC doc type
UNCLASSIFIED_FOLDER = "00_unclassified"

# Positive distinguishing features per doc type — used to enrich the classification prompt
_POSITIVE_FEATURES: dict[str, str] = {
    "bizfile": (
        "Official extract issued by a government corporate registry (e.g. ACRA in Singapore). "
        "Contains a unique registration number (UEN), registered address, principal business "
        "activity codes (SSIC), and company status. NOT a company's own self-introduction."
    ),
    "incorporation": (
        "Certificate of Incorporation or equivalent founding document issued by a government "
        "authority, bearing an official seal or stamp. Confirms legal formation of the entity."
    ),
    "maa": (
        "The company's constitution / memorandum & articles of association. Contains clauses on "
        "share capital, director appointment rules, shareholder meeting procedures, etc."
    ),
    "shareholders_directors": (
        "An official or board-certified list showing current shareholders (with shareholding "
        "percentages) and directors (with appointment dates). Typically on company letterhead."
    ),
    "ownership_structure": (
        "A chart or diagram showing the chain of ownership from ultimate beneficial owners (UBOs) "
        "down to the target company, with percentage holdings at each level. Usually signed by a director."
    ),
    "financial_reports": (
        "Audited annual reports, quarterly financial statements, or management accounts. Contains "
        "balance sheet, income statement, cash flow statement. Prepared by or reviewed by auditors."
    ),
    "bank_reference": (
        "A letter or statement issued by a bank confirming the company's account details, "
        "account standing, or banking relationship. On bank letterhead, issued within 12 months."
    ),
    "kyc_questionnaire": (
        "A structured questionnaire filled out and signed by the counterparty. Explicitly asks "
        "about business nature, source of funds, PEP status, sanctions exposure, and beneficial "
        "ownership. MUST have a signature block. NOT a company profile or brochure."
    ),
    "onboarding_form": (
        "An internal counterparty on-boarding application form (对家注册申请表). Contains fields "
        "for applicant department, proposed products, transaction background, and requires "
        "VP signature for approval."
    ),
    "source_of_funds": (
        "Documentary evidence proving the origin of funds — e.g. auditor confirmation letter, "
        "bank statements showing fund inflows, or board resolution on capital sources."
    ),
}


    # Codes excluded from AI classification (manual-only types)
MANUAL_ONLY_CODES = {"others"}


def _build_doc_type_descriptions() -> str:
    """Build enriched document-type descriptions for the classification prompt.

    Excludes manual-only types (e.g. 'others') that should never be
    suggested by the AI classifier.
    """
    lines = []
    for dt in sorted(DOC_TYPES.values(), key=lambda x: x.sort_order):
        if dt.code in MANUAL_ONLY_CODES:
            continue
        feature = _POSITIVE_FEATURES.get(dt.code, "")
        entry = f"- {dt.code}: {dt.name_en} ({dt.name_zh})"
        if feature:
            entry += f"\n  Key features: {feature}"
        lines.append(entry)
    return "\n".join(lines)


# Document type list for prompt (enriched with distinguishing features)
DOC_TYPE_DESCRIPTIONS = _build_doc_type_descriptions()
