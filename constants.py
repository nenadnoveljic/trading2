import os

NAME = 'Name'
PE = 'P / E'
PB = 'Price / Book Ratio'
PE_PB = 'PE*PB'
SYMBOL = 'Symbol' 

DIR = os.path.dirname(os.path.abspath(__file__))
PORTFOLIO_FILE = os.path.join(DIR, 'portfolio.csv')
