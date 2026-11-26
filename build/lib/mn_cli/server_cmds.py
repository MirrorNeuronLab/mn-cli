"""Compatibility facade for MirrorNeuron runtime launcher helpers.

The implementation now lives under :mod:`mn_cli.runtime`.  This module is kept
as an alias so existing imports and test monkeypatches against
``mn_cli.server_cmds`` continue to affect the runtime implementation.
"""

from mn_cli.runtime import server as _server
import sys as _sys

_sys.modules[__name__] = _server
