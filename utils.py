def is_prod():
    import os
    return os.environ.get("FOODDIARY_IS_PROD", default=None) == "1"