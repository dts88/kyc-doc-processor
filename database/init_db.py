"""Database initialization and seeding."""

from pathlib import Path

from database.connection import DatabaseManager

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

# 10 KYC document types (9 required + 1 optional)
KYC_DOC_TYPES = [
    (1, "bizfile", "Company Registration (Bizfile)", "公司注册资料（Bizfile）", 1, 1,
     "Official company registration extract from corporate registry"),
    (2, "incorporation", "Incorporation Documents", "公司成立文件", 1, 2,
     "Certificate of incorporation and related founding documents"),
    (3, "maa", "Memorandum and Articles of Association", "公司章程", 1, 3,
     "Company constitution, memorandum and articles of association"),
    (4, "shareholders_directors", "Shareholders & Directors List", "股东和董事名单", 1, 4,
     "List of shareholders (10%+ holdings) and directors"),
    (5, "ownership_structure", "Ownership/Org Structure Chart", "所有权/组织架构图", 1, 5,
     "Ownership structure chart showing UBOs, signed by director"),
    (6, "financial_reports", "Financial Reports", "审计报告和财报", 1, 6,
     "Audited reports for last 3 years + latest quarterly financial statements"),
    (7, "bank_reference", "Bank Account Reference", "银行账户资料", 1, 7,
     "Bank reference or account details issued within last 12 months"),
    (8, "kyc_questionnaire", "KYC Questionnaire", "KYC问卷", 1, 8,
     "Completed KYC questionnaire signed by authorized person"),
    (9, "onboarding_form", "Counterparty On-boarding Application Form", "对家注册申请表", 1, 9,
     "Internal application form with VP signature (scanned PDF) or confirmed via VP email"),
    (10, "source_of_funds", "Source of Funds Proof", "资金来源证明", 0, 10,
     "Documentary evidence of source of funds (optional)"),
    (11, "others", "Others", "其他文件", 0, 99,
     "Other documents belonging to this counterparty that do not fit any KYC category (manual-only)"),
]


def init_database(db: DatabaseManager) -> None:
    """Create tables and seed document types."""
    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    db.execute_script(schema_sql)

    # Seed document types
    with db.get_cursor() as cursor:
        for doc_type in KYC_DOC_TYPES:
            cursor.execute(
                """INSERT OR IGNORE INTO kyc_doc_types
                   (id, code, name_en, name_zh, required, sort_order, description)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                doc_type,
            )

    print("Database initialized successfully.")
    print(f"  - Database path: {db.db_path}")
    print(f"  - Seeded {len(KYC_DOC_TYPES)} KYC document types")
