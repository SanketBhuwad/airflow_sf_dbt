def decide_action(file_name: str) -> str:
    """
    Return 'truncate' if this is a customer file, otherwise 'append'.
    """
    base = file_name.lower()
    if "customer" in base:
        return "truncate"
    return "append"
