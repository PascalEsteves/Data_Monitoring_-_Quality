from pipeline.base import ValidationAction


class CheckNull(ValidationAction):

    action_name = "Check Null"

    def run(self):
        for check in self.params.get("checks", []):
            col:str = check.get("column")
            self.validate_column_exist(column=col)

            invalid = self.df[self.df[col].isnull() | (self.df[col].astype(str).str.strip() == "")]
            self.report({
                "Action": self.action_name,
                "Column": col,
                "Invalid_count": len(invalid),
                "Valid_Column" : len(invalid)==0,
                "Details": invalid.to_dict(orient="records")
            })
