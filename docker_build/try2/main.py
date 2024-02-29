from fund_data import *
from var import *
if __name__ == '__main__':
    database = Database(login3)
    product_update(database)
    each_product(database)
    print(get_value('final_text'))
