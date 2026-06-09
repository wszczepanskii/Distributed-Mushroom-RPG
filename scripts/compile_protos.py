"""
Compile protos/game.proto into generated/game_pb2.py and game_pb2_grpc.py.

Run from project root:
    python scripts/compile_protos.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTOS = ROOT / "protos"
OUT = ROOT / "generated"


def main() -> None:
    OUT.mkdir(exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTOS}",
        f"--python_out={OUT}",
        f"--grpc_python_out={OUT}",
        str(PROTOS / "game.proto"),
    ]
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)

    # Fix import path in generated gRPC stub.
    grpc_file = OUT / "game_pb2_grpc.py"
    text = grpc_file.read_text(encoding="utf-8")
    text = text.replace("import game_pb2", "from generated import game_pb2")
    grpc_file.write_text(text, encoding="utf-8")
    print("Proto compile complete -> generated/")


if __name__ == "__main__":
    main()
