from backtesting.EMAPullbackStrategy import EMAPullbackStrategy

def pretty(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))

test = EMAPullbackStrategy('XAUUSD', 2022, start_offset=0, trading_cost=0.1, pip_value=0.01)
test.test()

print(test.get_groupby_status())
print(pretty(test.get_results()))
print(test.get_pnl())
