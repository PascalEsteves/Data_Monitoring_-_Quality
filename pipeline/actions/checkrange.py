from pipeline.base import ValidationAction


class CheckPattern(ValidationAction):

    action_name  = "Check Range"
    
    def run(self):
        for check in self.params.get("checks", []):
            col:str = check.get("column")
            min_val:float = check.get("min")
            max_val:float = check.get("max")
            self.validate_column_exist(column=col)

            invalid = self.df[(self.df[col] < min_val) | (self.df[col] > max_val)]            
            self.report({
                "Action": self.action_name,
                "Column": col,
                "Invalid_count": len(invalid),
                "Valid_Column" : len(invalid)==0,
                "Details": invalid.to_dict(orient="records")
            })
