from pipeline.base import ValidationAction


class CheckUnique(ValidationAction):

    action_name  = "Check Unique"

    def run(self):
        for check in self.params.get("checks",[]):
            col:str = check.get("column")
            self.validate_column_exist(col)

            invalid = self.df[self.df[col].duplicated(keep=False)]
            self.report({
                "Action": self.action_name,
                "Column": col,
                "Invalid_count": len(invalid),
                "Valid_Column" : len(invalid)==0,
                "Details": invalid.to_dict(orient="records")
            })
