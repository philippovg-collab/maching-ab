from typing import Dict, Set

ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "admin": {
        "ingest:write",
        "ingest:read",
        "match:execute",
        "match:read",
        "exceptions:read",
        "exceptions:write",
        "admin:rules",
        "audit:read",
        "analytics:read",
    },
    "operator_l1": {"match:read", "exceptions:read", "exceptions:write", "analytics:read"},
    "operator_l2": {"match:read", "exceptions:read", "exceptions:write", "analytics:read"},
    "auditor": {"audit:read", "exceptions:read", "match:read", "analytics:read"},
    "finance_viewer": {"match:read", "exceptions:read", "analytics:read"},
}


def has_permission(roles: Set[str], permission: str) -> bool:
    for role in roles:
        if permission in ROLE_PERMISSIONS.get(role, set()):
            return True
    return False
