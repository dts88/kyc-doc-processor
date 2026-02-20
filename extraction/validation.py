"""Business rule validation for extracted KYC data."""

import re
from datetime import datetime, timedelta

# Configurable thresholds (can be overridden via set_validation_config)
_config = {
    "shareholder_min_percentage": 10,
    "bank_reference_max_age_days": 365,
    "known_vp_names": [],  # loaded from vp_signatures table
}


def set_validation_config(config: dict):
    """Update validation thresholds from config.yaml."""
    if "validation" in config:
        v = config["validation"]
        if "shareholder_min_percentage" in v:
            _config["shareholder_min_percentage"] = v["shareholder_min_percentage"]
        if "bank_reference_max_age_days" in v:
            _config["bank_reference_max_age_days"] = v["bank_reference_max_age_days"]
    if "known_vp_names" in config:
        _config["known_vp_names"] = config["known_vp_names"]


def load_vp_names_from_db(db) -> None:
    """Load active VP names from vp_signatures table into validation config."""
    try:
        from web.services.vp_service import get_active_vp_names
        _config["known_vp_names"] = get_active_vp_names(db)
    except Exception:
        pass


def validate_extraction(doc_type: str, data: dict) -> tuple[bool, list[str]]:
    """Validate extracted data against business rules.

    Returns (passed, list_of_errors).
    """
    errors = []
    validator = VALIDATORS.get(doc_type)
    if validator:
        errors = validator(data)
    return len(errors) == 0, errors


def _validate_date_format(date_str: str | None, field_name: str) -> str | None:
    """Check if date is in YYYY-MM-DD format."""
    if not date_str:
        return None
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return f"{field_name}: invalid date format '{date_str}', expected YYYY-MM-DD"
    return None


def _validate_bizfile(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    if not data.get("registration_number"):
        errors.append("Registration number is required")
    err = _validate_date_format(data.get("incorporation_date"), "incorporation_date")
    if err:
        errors.append(err)
    return errors


def _validate_incorporation(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    if not data.get("incorporation_date"):
        errors.append("Incorporation date is required")
    err = _validate_date_format(data.get("incorporation_date"), "incorporation_date")
    if err:
        errors.append(err)
    return errors


def _validate_shareholders_directors(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    directors = data.get("directors", [])
    shareholders = data.get("shareholders", [])
    if not directors:
        errors.append("At least one director is expected")
    if not shareholders:
        errors.append("At least one shareholder is expected")
    # Check that listed shareholders meet minimum ownership threshold
    min_pct = _config["shareholder_min_percentage"]
    for sh in shareholders:
        pct = sh.get("percentage")
        if pct is not None and pct < min_pct:
            errors.append(f"Shareholder '{sh.get('name')}' has {pct}% - below {min_pct}% threshold")
    return errors


def _validate_ownership_structure(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    if not data.get("ultimate_beneficial_owners"):
        errors.append("At least one ultimate beneficial owner expected")
    if data.get("director_signed") is False:
        errors.append("Warning: ownership chart not signed by director")
    return errors


def _validate_financial_reports(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    if not data.get("period_covered"):
        errors.append("Reporting period is required")
    return errors


def _validate_bank_reference(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name / account holder is required")
    if not data.get("bank_name"):
        errors.append("Bank name is required")
    # Check if reference is within max age
    max_age = _config["bank_reference_max_age_days"]
    ref_date = data.get("reference_date")
    if ref_date:
        try:
            ref_dt = datetime.strptime(ref_date, "%Y-%m-%d")
            if datetime.now() - ref_dt > timedelta(days=max_age):
                errors.append(f"Bank reference dated {ref_date} is older than {max_age} days")
        except ValueError:
            errors.append(f"Invalid reference_date format: {ref_date}")
    return errors


def _validate_kyc_questionnaire(data: dict) -> list[str]:
    errors = []
    if not data.get("company_name"):
        errors.append("Company name is required")
    if not data.get("authorized_signatory"):
        errors.append("Authorized signatory is required")
    if not data.get("date_signed"):
        errors.append("Signing date is required")
    return errors


def _validate_onboarding_form(data: dict) -> list[str]:
    errors = []
    if not data.get("counterparty_name_zh") and not data.get("counterparty_name_en"):
        errors.append("Counterparty name (Chinese or English) is required")
    if not data.get("applicant"):
        errors.append("Applicant name is required")
    if not data.get("application_date"):
        errors.append("Application date is required")
    err = _validate_date_format(data.get("application_date"), "application_date")
    if err:
        errors.append(err)
    # VP signature validation
    vp_name = data.get("vp_signature_name")
    if data.get("vp_signed") is False and not vp_name:
        errors.append("Warning: VP signature not detected on form")
    elif vp_name and _config["known_vp_names"]:
        # Validate against known VP list (case-insensitive)
        known = [n.lower() for n in _config["known_vp_names"]]
        if vp_name.lower() not in known:
            errors.append(
                f"Warning: VP signer '{vp_name}' not in known VP list "
                f"({', '.join(_config['known_vp_names'])})"
            )
    return errors


def _validate_source_of_funds(data: dict) -> list[str]:
    errors = []
    if not data.get("fund_sources"):
        errors.append("At least one fund source should be identified")
    return errors


VALIDATORS = {
    "bizfile": _validate_bizfile,
    "incorporation": _validate_incorporation,
    "maa": lambda d: [e for e in ["Company name is required"] if not d.get("company_name")] or [],
    "shareholders_directors": _validate_shareholders_directors,
    "ownership_structure": _validate_ownership_structure,
    "financial_reports": _validate_financial_reports,
    "bank_reference": _validate_bank_reference,
    "kyc_questionnaire": _validate_kyc_questionnaire,
    "onboarding_form": _validate_onboarding_form,
    "source_of_funds": _validate_source_of_funds,
}
