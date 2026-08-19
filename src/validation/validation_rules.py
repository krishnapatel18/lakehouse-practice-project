from datetime import datetime

# Customer table validation rules
CUSTOMER_VALIDATIONS = {
    "numeric_min": {
        "customer_id": 0  # Must be non-negative
    },
    "regex": {
        "name": r"^[a-zA-Z ]+$",
        "email": r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$",
        "city": r"^[a-zA-Z ]+$",
        "state": r"^[a-zA-Z ]+$"
    },
    "date_min": {
        "signup_date": datetime(1900, 1, 1),
        "created_date": datetime(1900, 1, 1),
        "modified_date": datetime(1900, 1, 1)
    }
}
