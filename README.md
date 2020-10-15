# Case parser for sudrf.ru

## Usage

`./court_bot.py -h`

```
usage: court_bot.py [-h] [-v VERBOSE] -r REGION -y YEARS
                    [-p {http,socks4,socks5}]

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSE, --verbose VERBOSE
                        Logging level, for example: DEBUG or WARNING.
  -r REGION, --region REGION
                        Provides region number to parse.
  -y YEARS, --year YEARS
                        Provides year to parse.
  -p {http,socks4,socks5}, --proxytype {http,socks4,socks5}
                        Set of proxy type.
```

## Example

Parse cases for 35 region 2018-2020 years:

`./court_bot.py -r 35 -y 2018 -y 2019 -y 2020`
