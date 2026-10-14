"""JSON worker entry; modules can be shipped in-memory by cli.py."""
import json
import importlib.util
from pathlib import Path
import sys

# Direct python3 worker.py invocation uses only these owned sibling modules.
for name in ("engine", "nas", "build"):
    if "reolink_deploy_" + name not in sys.modules:
        specification = importlib.util.spec_from_file_location("reolink_deploy_" + name, Path(__file__).resolve().with_name(name + ".py"))
        module = importlib.util.module_from_spec(specification)
        sys.modules[specification.name] = module
        specification.loader.exec_module(module)

from reolink_deploy_engine import DeployEngine, GuardError
from reolink_deploy_nas import DockerNas
from reolink_deploy_build import AuditBuilder


def run(payload):
    try:
        backend = DockerNas(payload.get("settings"))
        return AuditBuilder(backend).run(payload) if payload.get("mode") == "build" else DeployEngine(backend).run(payload)
    except Exception as error:
        return {"ok": False, "mode": payload.get("mode"), "errorCode": str(error) if isinstance(error, GuardError) else "deployment_worker_failed",
                "errorType": type(error).__name__, "credentialsPrinted": False}


if __name__ == "__main__":
    result = run(json.load(sys.stdin))
    print(json.dumps(result))
    raise SystemExit(0 if result.get("ok") else 1)
