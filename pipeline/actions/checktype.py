from pipeline.base import ValidationAction
import datetime
import pandas as pd

class CheckType(ValidationAction):
    action_name = "Check Type"
    TYPE_MAP = {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "datetime": datetime.datetime,
        "date": datetime.date
    }
    
    def run(self):
        for check in self.params.get("checks", []):
            col: str = check.get("column")
            type: str = check.get("type")
            self.validate_column_exist(column=col)

            def is_valid_type(x):
                if type in ["datetime", "date"]:
                    if isinstance(x, self.TYPE_MAP[type]):
                        return True
                    try:
                        parsed_date = pd.to_datetime(x, errors='raise', dayfirst=True)
                        if type == "date":
                            return isinstance(parsed_date.date(), datetime.date)
                        return True
                    except Exception:
                        return False
                return isinstance(x, self.TYPE_MAP[type])

            invalid = self.df[~self.df[col].apply(is_valid_type)]

            self.report({
                "Action": self.action_name,
                "Column": col,
                "Invalid_count": len(invalid),
                "Valid_Column": len(invalid) == 0,
                "Details": invalid.to_dict(orient="records")
            })
