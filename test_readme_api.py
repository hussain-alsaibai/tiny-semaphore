"""The README must document an API that actually exists.

This test exists because the README told readers to write

    from tiny_semaphore import Semaphore

while `tiny_semaphore` exports `AsyncSemaphore(limit, fairness)` and the
`Semaphore(max_permits, name)` class the examples were actually written
against lives in `semaphore.py`. Every example in the README raised
ImportError before this was fixed, and nothing caught it because no test
ever executed the README.

The guard is deliberately narrow: for every `from <mod> import <names>`
line inside a python code fence, the module must import and each name
must exist on it. That is the cheapest possible check for the whole class
of bug, and it is the one that was missing.
"""

from __future__ import annotations

import importlib
import os
import re
import sys

import pytest

README = os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md")
BLOCK = re.compile(r"```python\n(.*?)```", re.S)
IMPORT_LINE = re.compile(r"^from\s+([\w.]+)\s+import\s+(.+)$", re.M)


def _blocks():
    if not os.path.exists(README):
        pytest.skip("no README.md")
    with open(README, encoding="utf-8") as fh:
        text = fh.read()
    return BLOCK.findall(text)


def _import_lines():
    """Yield (block_index, module, [names]) for every import in the README."""
    for i, block in enumerate(_blocks()):
        for match in IMPORT_LINE.finditer(block):
            module = match.group(1)
            names = []
            for raw in match.group(2).split(","):
                name = raw.strip().split(" as ")[0].strip()
                if name and name.isidentifier():
                    names.append(name)
            if names:
                yield i, module, names


def _compiles(block):
    """Compile a README block, allowing top-level await.

    A README legitimately shows `results = await asyncio.gather(...)` at
    module level, which is only valid inside a coroutine. We wrap the block
    in an async function before compiling so the example is judged on its
    real syntax, not on the fact that it is a fragment of a script.
    """
    try:
        compile(block, "<readme>", "exec")
        return True
    except SyntaxError:
        pass
    indented = "".join(
        "    " + line if line.strip() else line for line in block.splitlines(True)
    )
    try:
        compile("async def _readme_probe():\n" + indented, "<readme>", "exec")
        return True
    except SyntaxError:
        return False


def test_readme_python_blocks_compile():
    """A README example that cannot compile is a wrong README."""
    for i, block in enumerate(_blocks()):
        assert _compiles(block), "README block %d does not compile" % i


def test_every_readme_import_resolves():
    """Every name the README imports must exist on the module it names."""
    sys.path.insert(0, os.path.dirname(README))
    problems = []
    for block, module, names in _import_lines():
        try:
            mod = importlib.import_module(module)
        except Exception as exc:
            problems.append("block %d: cannot import %s (%s: %s)"
                            % (block, module, type(exc).__name__, exc))
            continue
        missing = [n for n in names if not hasattr(mod, n)]
        if missing:
            problems.append("block %d: %s has no %s"
                            % (block, module, ", ".join(missing)))
    assert not problems, "README documents a non-existent API:\n  " + "\n  ".join(problems)


def test_readme_does_not_import_semaphore_from_wrong_module():
    """Regression: the examples were written against semaphore.py.

    `tiny_semaphore` never exported `Semaphore`, so importing it from there
    was always an ImportError. If a future edit reintroduces that line this
    test names the reason instead of leaving a bare ImportError.
    """
    offenders = [
        b for b, module, names in _import_lines()
        if module == "tiny_semaphore" and "Semaphore" in names
    ]
    assert not offenders, (
        "README imports Semaphore from tiny_semaphore, but that module exports "
        "AsyncSemaphore(limit, fairness). Semaphore(max_permits, name) lives in "
        "semaphore.py. Offending block(s): %s" % offenders
    )
