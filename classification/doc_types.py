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
}

# Required document type codes
REQUIRED_DOC_CODES = [code for code, dt in DOC_TYPES.items() if dt.required]

# Map from ID to code
ID_TO_CODE = {dt.id: code for code, dt in DOC_TYPES.items()}
CODE_TO_ID = {code: dt.id for code, dt in DOC_TYPES.items()}

# Document type list for prompt
DOC_TYPE_DESCRIPTIONS = "\n".join(
    f"- {dt.code}: {dt.name_en} ({dt.name_zh})"
    for dt in sorted(DOC_TYPES.values(), key=lambda x: x.sort_order)
)
