"""Invoke owned deployment code through an external user-protected NAS connector."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shlex
import sys

HERE = Path(__file__).resolve().parent
BOOTSTRAP = """import json,sys,types
b=json.load(sys.stdin)
for name in ('engine','nas','build','worker'):
 module=types.ModuleType('reolink_deploy_'+name)
 module.__file__='<owned-deploy-'+name+'>'
 sys.modules[module.__name__]=module
 exec(compile(b['sources'][name],module.__file__,'exec'),module.__dict__)
print(json.dumps(sys.modules['reolink_deploy_worker'].run(b['payload'])))
"""


MODULE_NAMES = ("engine", "nas", "build", "worker")


def source_bundle():
    return {name: (HERE / (name + ".py")).read_text(encoding="utf-8") for name in MODULE_NAMES}


def invoke(payload, connector, sources=None):
    path = Path(connector).expanduser().resolve()
    if not path.is_file() or path.is_symlink() or HERE.parent.parent.resolve() in path.parents:
        raise ValueError("external_protected_connector_required")
    specification = importlib.util.spec_from_file_location("protected_nas_access", path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    if not callable(getattr(module, "nas_execute", None)):
        raise ValueError("nas_execute_callback_required")
    frozen_sources = source_bundle() if sources is None else dict(sources)
    if set(frozen_sources) != set(MODULE_NAMES) or not all(isinstance(value, str) and 0 < len(value) <= 1024 * 1024 for value in frozen_sources.values()):
        raise ValueError("owned_worker_module_bundle_required")
    bundle = {"payload": payload, "sources": frozen_sources}
    response = module.nas_execute("python3 -c " + shlex.quote(BOOTSTRAP), payload=json.dumps(bundle), sudo=True, timeout=6000 if payload.get("mode") == "build" else 900)
    if response.get("exit_code") != 0:
        raise ValueError("protected_connector_execution_failed")
    result = json.loads(response["stdout"])
    if not isinstance(result, dict) or not isinstance(result.get("ok"), bool):
        raise ValueError("worker_result_invalid")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=Path, required=True, help="JSON request; no credentials are needed in this file")
    parser.add_argument("--connector", default=os.environ.get("REOLINK_NAS_CONNECTOR"), help="External protected access.py exposing nas_execute")
    parser.add_argument("--sources", type=Path, help="Previously frozen public module-text JSON, including the recovery worker")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.connector or args.output.exists() or args.output.is_symlink():
        parser.error("An external connector and fresh output are required")
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    sources = json.loads(args.sources.read_text(encoding="utf-8")) if args.sources else None
    result = invoke(payload, args.connector, sources=sources)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2); stream.write("\n")
    print(json.dumps({"ok": result.get("ok"), "mode": result.get("mode"), "output": str(args.output), "credentialsPrinted": False}))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"ok": False, "errorCode": "deployment_cli_failed", "errorType": type(error).__name__, "credentialsPrinted": False}))
        raise SystemExit(1)
