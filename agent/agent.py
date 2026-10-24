#!/usr/bin/env python3
"""Entry point kept for compatibility: `python agent/agent.py` == `python -m mdjaada_agent`."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mdjaada_agent.main import main  # noqa: E402

if __name__ == "__main__":
    main()
