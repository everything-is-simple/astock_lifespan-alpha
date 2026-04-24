from __future__ import annotations

import importlib
import sys


def test_malf_public_import_does_not_load_audit_dependencies():
    for module_name in list(sys.modules):
        if module_name == "astock_lifespan_alpha.malf" or module_name.startswith(
            "astock_lifespan_alpha.malf."
        ):
            sys.modules.pop(module_name)

    malf_module = importlib.import_module("astock_lifespan_alpha.malf")

    assert hasattr(malf_module, "run_malf_day_build")
    assert "astock_lifespan_alpha.malf.audit" not in sys.modules
