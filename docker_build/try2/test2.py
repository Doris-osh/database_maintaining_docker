from flask import Flask, render_template
from fund_data import *
import time
app = Flask(__name__)
from var import *
import  warnings
warnings.filterwarnings('ignore')

@app.route('/')
def index():
  return render_template('index.html')

@app.route('/run-python/')
def my_link():
  database = Database(login3)
  try:
    product_update(database)
    each_product(database)
    return render_template('index1.html', final_text = get_value('final_text'))
  except:
    return render_template('index1.html', final_text = get_value('final_text'))

if __name__ == '__main__':
  app.run(host='0.0.0.0', debug=True)