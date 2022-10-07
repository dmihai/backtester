from backtesting.strategies.TrendCycleEntry import TrendCycleEntry

def pretty(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))

test = TrendCycleEntry('EURUSD', 2021)
test.test()

print(test.get_groupby_status())
print(pretty(test.get_results()))
print(test.get_pnl())
