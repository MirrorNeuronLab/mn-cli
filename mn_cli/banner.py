MN_ASCII_ART = r"""
  __  __ _                     _   _                           
 |  \/  (_)_ __ _ __ ___  _ __| \ | | ___ _   _ _ __ ___  _ __ 
 | |\/| | | '__| '__/ _ \| '__|  \| |/ _ \ | | | '__/ _ \| '_ \
 | |  | | | |  | | | (_) | |  | |\  |  __/ |_| | | | (_) | | | |
 |_|  |_|_|_|  |_|  \___/|_|  |_| \_|\___|\__,_|_|  \___/|_| |_|
""".strip("\n")

COMPACT_BANNER_MAX_WIDTH = 72


def _compact_title(title: str | None, *, width: int) -> str:
    value = str(title or "MirrorNeuron CLI")
    if width <= 0:
        return value
    if len(value) <= width:
        return value
    if width == 1:
        return "…"
    return value[: width - 1].rstrip() + "…"


def format_banner(title: str | None = None, *, width: int | None = None) -> str:
    if width is not None and width < COMPACT_BANNER_MAX_WIDTH:
        return _compact_title(title, width=width)
    if not title:
        return MN_ASCII_ART
    return f"{MN_ASCII_ART}\n\n => {title}"
