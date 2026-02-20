"""Pydantic models for structured data extracted from KYC documents."""

from pydantic import BaseModel, Field


class BizfileData(BaseModel):
    company_name: str | None = None
    registration_number: str | None = None
    incorporation_date: str | None = None
    company_type: str | None = None
    registered_address: str | None = None
    company_status: str | None = None
    principal_activities: list[str] = Field(default_factory=list)
    paid_up_capital: str | None = None


class IncorporationData(BaseModel):
    company_name: str | None = None
    registration_number: str | None = None
    incorporation_date: str | None = None
    jurisdiction: str | None = None
    company_type: str | None = None
    authorized_share_capital: str | None = None
    initial_directors: list[str] = Field(default_factory=list)
    initial_shareholders: list[str] = Field(default_factory=list)


class MAAData(BaseModel):
    company_name: str | None = None
    date_adopted: str | None = None
    share_classes: list[str] = Field(default_factory=list)
    director_appointment_rules: str | None = None
    key_provisions: list[str] = Field(default_factory=list)
    amendment_date: str | None = None


class DirectorInfo(BaseModel):
    name: str | None = None
    nationality: str | None = None
    id_number: str | None = None
    appointment_date: str | None = None


class ShareholderInfo(BaseModel):
    name: str | None = None
    shares: int | None = None
    percentage: float | None = None
    share_type: str | None = None


class ShareholdersDirectorsData(BaseModel):
    company_name: str | None = None
    document_date: str | None = None
    directors: list[DirectorInfo] = Field(default_factory=list)
    shareholders: list[ShareholderInfo] = Field(default_factory=list)
    total_shares: int | None = None
    secretary: str | None = None


class UBOInfo(BaseModel):
    name: str | None = None
    nationality: str | None = None
    percentage: float | None = None


class IntermediateEntity(BaseModel):
    name: str | None = None
    jurisdiction: str | None = None
    ownership_percentage: float | None = None
    parent: str | None = None


class OwnershipStructureData(BaseModel):
    company_name: str | None = None
    ultimate_beneficial_owners: list[UBOInfo] = Field(default_factory=list)
    intermediate_entities: list[IntermediateEntity] = Field(default_factory=list)
    director_signed: bool | None = None
    signing_director_name: str | None = None
    document_date: str | None = None


class FinancialReportData(BaseModel):
    company_name: str | None = None
    report_type: str | None = None
    fiscal_year_end: str | None = None
    period_covered: str | None = None
    auditor_name: str | None = None
    audit_opinion: str | None = None
    total_revenue: str | None = None
    net_profit_loss: str | None = None
    total_assets: str | None = None
    total_liabilities: str | None = None
    total_equity: str | None = None
    currency: str | None = None


class BankReferenceData(BaseModel):
    company_name: str | None = None
    bank_name: str | None = None
    branch: str | None = None
    account_number: str | None = None
    account_type: str | None = None
    currency: str | None = None
    date_opened: str | None = None
    reference_date: str | None = None
    signatory: str | None = None


class KYCQuestionnaireData(BaseModel):
    company_name: str | None = None
    authorized_signatory: str | None = None
    signatory_title: str | None = None
    date_signed: str | None = None
    business_nature: str | None = None
    countries_of_operation: list[str] = Field(default_factory=list)
    expected_transaction_volume: str | None = None
    source_of_funds: str | None = None
    pep_declarations: str | None = None
    sanctions_screening: str | None = None


class OnboardingFormData(BaseModel):
    counterparty_name_zh: str | None = None
    counterparty_name_en: str | None = None
    incorporation_date: str | None = None
    business_nature: str | None = None
    legal_representative: str | None = None
    business_scope: str | None = None
    registered_capital: str | None = None
    net_assets: str | None = None
    registered_address: str | None = None
    contact_person: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    proposed_products: str | None = None
    transaction_background: str | None = None
    business_supervisor: str | None = None
    applicant: str | None = None
    department: str | None = None
    application_date: str | None = None
    urgency: str | None = None
    vp_signed: bool | None = None
    vp_signature_name: str | None = None
    onboarder_review: str | None = None
    risk_management_review: str | None = None


class SourceOfFundsData(BaseModel):
    company_name: str | None = None
    fund_sources: list[str] = Field(default_factory=list)
    evidence_type: str | None = None
    total_amount: str | None = None
    period_covered: str | None = None
    supporting_documents: list[str] = Field(default_factory=list)
    certified_by: str | None = None


# Map doc_type code to Pydantic model
DOC_TYPE_SCHEMA_MAP = {
    "bizfile": BizfileData,
    "incorporation": IncorporationData,
    "maa": MAAData,
    "shareholders_directors": ShareholdersDirectorsData,
    "ownership_structure": OwnershipStructureData,
    "financial_reports": FinancialReportData,
    "bank_reference": BankReferenceData,
    "kyc_questionnaire": KYCQuestionnaireData,
    "onboarding_form": OnboardingFormData,
    "source_of_funds": SourceOfFundsData,
}
