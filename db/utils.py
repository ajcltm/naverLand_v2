from datetime import datetime

class InsertFormatter:

    def get_values_parts(self, data:list):
        values_part_lst = [self.get_values_part(i.dict()) for i in data]
        values_part = ', '.join(values_part_lst)
        return values_part

    def get_values_part(self, data:dict):
        values = data.values()
        values_part_lst = [self.get_string_format(value) for value in values]
        values_part = ', '.join(values_part_lst)
        return f'({values_part})'

    def get_string_format(self, value:any)->str:
        if type(value) == str:
            value = value.replace("'", '')
            value = value.replace('"', '')
            value = value.replace('\\', '')
            return f"'{value}'"

        elif type(value)==datetime:
            return f"'{value.strftime(format='%Y-%m-%d')}'"

        elif value == None:
            return "Null"

        else:
            return f'{value}'