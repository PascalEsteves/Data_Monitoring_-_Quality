import pandas as pd
from pipeline.base import ValidationAction

class CheckDateInterval(ValidationAction):

    action_name  = "Check Date Interval"

    def run(self):
        col = self.params["column"]
        start = pd.to_datetime(self.params["start"])
        end = pd.to_datetime(self.params["end"])
        self.validate_column_exist(column=col)

        dates = pd.to_datetime(self.df[col], errors="coerce")
        invalid = self.df[(dates < start) | (dates > end) | dates.isna()]
        self.report({
            "Action": self.action_name ,
            "Column": col,
            "Start": str(start),
            "End": str(end),
            "Invalid_count": len(invalid),
            "Valid_Column" : len(invalid)==0,
            "Details": invalid.to_dict(orient="records")
        })