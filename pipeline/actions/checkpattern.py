from pipeline.base import ValidationAction


class CheckPattern(ValidationAction):

    action_name  = "Check Pattern"
    
    def run(self):
        for check in self.params.get("checks", []):
            col:str = check.get("column")
            pattern:str = check.get("pattern")
            self.validate_column_exist(column=col)

            invalid = self.df[~self.df[col].astype(str).str.match(pattern)]
            self.report({
                "Action": self.action_name,
                "Column": col,
                "Invalid_count": len(invalid),
                "Valid_Column" : len(invalid)==0,
                "Details": invalid.to_dict(orient="records")
            })
