# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Dedicated graph target, deliberately independent of the plain Valkey harness."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for source in ("glide-shared", "glide-async/python", "glide-sync"):
    sys.path.insert(0, str(ROOT / source))
