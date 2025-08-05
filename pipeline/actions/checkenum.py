from pipeline.base import ValidationAction

class CheckEnum(ValidationAction):

    action_name  = "Check Enum"
    
    def run(self):
        col = self.params["column"]
        allowed = self.params["allowed_values"]
        self.validate_column_exist(col)

        invalid = self.df[~self.df[col].isin(allowed)]
        self.report({
            "Action": self.action_name ,
            "Column": col,
            "Allowed_values": allowed,
            "Invalid_count": len(invalid),
            "Valid_Column" : len(invalid)==0,
            "Details": invalid.to_dict(orient="records")
        })
