MN_ASCII_ART = r"""
  __  __ _                     _   _                           
 |  \/  (_)_ __ _ __ ___  _ __| \ | | ___ _   _ _ __ ___  _ __ 
 | |\/| | | '__| '__/ _ \| '__|  \| |/ _ \ | | | '__/ _ \| '_ \
 | |  | | | |  | | | (_) | |  | |\  |  __/ |_| | | | (_) | | | |
 |_|  |_|_|_|  |_|  \___/|_|  |_| \_|\___|\__,_|_|  \___/|_| |_|
""".strip("\n")


def format_banner(title: str | None = None) -> str:
    if not title:
        return MN_ASCII_ART
    return f"{MN_ASCII_ART}\n\n => {title}"
