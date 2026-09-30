"""Generate Python protobuf/gRPC stubs (+ mypy .pyi) into src/ingestion/generated/.

protoc emits absolute imports such as ``from events.v1 import events_pb2``. Those only
work if ``generated/`` is on sys.path, which would also make every message importable
under two module names (and break the protobuf descriptor pool). This script therefore
rewrites them to ``from ingestion.generated.events.v1 import ...`` as a post-processing
step. The generated files are build output: never edit them by hand, re-run `make proto`.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
from pathlib import Path

from grpc_tools import protoc

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROTO_ROOT = PROJECT_ROOT / "proto"
OUTPUT_ROOT = PROJECT_ROOT / "src" / "ingestion" / "generated"
GENERATED_PACKAGE = "ingestion.generated"
PROTO_PACKAGES = ("events",)

_FROM_IMPORT = re.compile(r"^from (events(?:\.\w+)*) import ", re.MULTILINE)
_PLAIN_IMPORT = re.compile(r"^import (events(?:\.\w+)*)", re.MULTILINE)


def _run_protoc(proto_files: list[Path]) -> None:
    # protoc-gen-mypy / protoc-gen-mypy_grpc are installed next to the interpreter.
    interpreter_bin = str(Path(sys.executable).parent)
    os.environ["PATH"] = os.pathsep.join([interpreter_bin, os.environ.get("PATH", "")])
    grpc_tools_include = Path(protoc.__file__).parent / "_proto"
    arguments = [
        "grpc_tools.protoc",
        f"-I{PROTO_ROOT}",
        f"-I{grpc_tools_include}",
        f"--python_out={OUTPUT_ROOT}",
        f"--grpc_python_out={OUTPUT_ROOT}",
        f"--mypy_out={OUTPUT_ROOT}",
        f"--mypy_grpc_out={OUTPUT_ROOT}",
        *(str(proto_file) for proto_file in proto_files),
    ]
    exit_code = protoc.main(arguments)
    if exit_code != 0:
        raise SystemExit(f"protoc failed with exit code {exit_code}")


def _rewrite_imports(generated_file: Path) -> None:
    source = generated_file.read_text()
    rewritten = _FROM_IMPORT.sub(rf"from {GENERATED_PACKAGE}.\1 import ", source)
    rewritten = _PLAIN_IMPORT.sub(rf"import {GENERATED_PACKAGE}.\1", rewritten)
    # .pyi stubs reference fully-qualified names like `events.v1.events_pb2.X`.
    rewritten = re.sub(
        r"(?<![\w.])events\.v1\.(\w+_pb2)", rf"{GENERATED_PACKAGE}.events.v1.\1", rewritten
    )
    rewritten = rewritten.replace(
        f"import {GENERATED_PACKAGE}.{GENERATED_PACKAGE}.", f"import {GENERATED_PACKAGE}."
    )
    if rewritten != source:
        generated_file.write_text(rewritten)


def main() -> int:
    for package in PROTO_PACKAGES:
        shutil.rmtree(OUTPUT_ROOT / package, ignore_errors=True)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    proto_files = sorted(PROTO_ROOT.rglob("*.proto"))
    if not proto_files:
        raise SystemExit(f"no .proto files found under {PROTO_ROOT}")
    _run_protoc(proto_files)

    for generated_dir in [OUTPUT_ROOT, *(p for p in OUTPUT_ROOT.rglob("*") if p.is_dir())]:
        if "__pycache__" in generated_dir.parts:
            continue
        (generated_dir / "__init__.py").touch()
    for generated_file in OUTPUT_ROOT.rglob("*_pb2*.py*"):
        _rewrite_imports(generated_file)

    print(f"generated stubs for {len(proto_files)} proto files into {OUTPUT_ROOT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
