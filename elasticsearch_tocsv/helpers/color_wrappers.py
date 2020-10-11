def wrap_purple(string):
  return f'\033[95m{string}\033[0m'

def wrap_blue(string):
  return f'\033[94m{string}\033[0m'

def wrap_green(string):
  return f'\033[92m{string}\033[0m'

def wrap_red(string):
  return f'\033[91m{string}\033[0m'

def wrap_orange(string):
  return f'\033[93m{string}\033[0m'
  
def bold(string):
  return f'\033[1m{string}\033[0m'

def underline(string):
  return f'\033[4m{string}\033[0m'