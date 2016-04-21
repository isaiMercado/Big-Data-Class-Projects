# -*- coding: utf-8 -*-
"""
this code makes a static class like this

class BARBUDA(PLACE):
    LIST = ['antigua','barbuda','bird','bishop','blake','cinnamon','codrington','crump','dulcina','exchange','five','great bird',
    'green','guiana','hale gate','hawes','henry','johnson','kid','laviscounts','lobster','long','maid','moor','nanny','pelican',
    'prickly','rabbit','rat','red head','redonda','sandy','smith','sisters','vernon','wicked','york island']
    COUNTRY = "barbuda"


"""

import pycountry
from unidecode import unidecode
import re

country_name = "Spain"
country = pycountry.countries.get(name=country_name)

counter = 0
string = "["
for state in pycountry.subdivisions.get(country_code=country.alpha2):
    if counter > 10:
        string = string + "\n\t"
        counter = 0
    state = unidecode(state.name.lower())
    parts = re.findall(r"[\w']+", state)
    for part in parts:
        if len(part) > 4:
            string = string + "'" + part + "',"
            counter = counter + 1
            
string = string[:-1]
string = string + "]"

output = "class " + country.name.upper() + """(PLACE):
\tLIST = """ + string + """
\tCOUNTRY = '""" + country.name.lower() + "'"

output_file = open("/home/isai/Documents/classes/Bigdata2/substring_lists/list.txt", "w")
output_file.write(output)
