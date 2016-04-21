# coding=utf-8

from pyspark import SparkConf, SparkContext
from unidecode import unidecode
import pycountry
import json
import os
import re



#============================== Global ====================================



class FACT_TYPES:

    BIRTH_FACT = 'http://gedcomx.org/Birth'
    CHRISTENING_FACT = 'http://gedcomx.org/Christening'
    DEATH_FACT = 'http://gedcomx.org/Death'
    BURIAL_FACT = 'http://gedcomx.org/Burial'
    RESIDENCE_FACT = 'http://gedcomx.org/Residence'



class RELATIONSHIP_TYPES:

    CHILD = 'http://gedcomx.org/ParentChild'
    COUPLE = 'http://gedcomx.org/Couple'
    MARRIAGE = "http://gedcomx.org/Marriage"
    
    
    
class INFO_TYPE:

    NORMALIZED = "normalized"
    FORMAL = "formal"
    ORIGINAL = "original"
    
    
    
class Person(object):
    
    def __init__(self):
        self.birth_country = None
        self.birth_year = None
        self.death_country = None
        self.death_year = None



BUCKET_SIZE = 50;


# 49103 UNKNOWNS with all countries from pycountry
# 46718 UNKNOWNS just with europe and america countries with different spellings and capitals(using unidecode)


def extract_country(string):
    
    
    string = " " + re.sub(r"[^\w]+", ' ', string).lower() + " "

    
    for country in pycountry.countries:
        country_name = country.name.lower()
        country_alpha2 = " " + country.alpha2.lower() + " "
        country_alpha3 = " " + country.alpha3.lower() + " "
        if country_name in string or country_alpha2 in string or country_alpha3 in string:
            return unidecode(country_name).replace("'", "")
            
    
#    for state in pycountry.subdivisions:
#        words = re.findall(r"[\w']+", state.name.lower())
#        for word in words:
#            if len(word) > 4:
#                if word in string:
#                    return state.country.name.lower()
    
    
    for state in pycountry.subdivisions:
        if state.name.lower() in string:
            return unidecode(state.country.name.lower()).replace("'", "")
              
              
    return None




# This function receives a list and returns the entry that appears more times in the list
def argmax(input_list):
    counter = dict()
    for item in input_list:
        if item in counter:
            counter[item] += 1
        else:
            counter[item] = 1
            
    argmax_key = ""
    argmax_value = 0
    for (key, value) in counter.items():
        if value > argmax_value:
            argmax_key = key
            argmax_value = value
            
    return argmax_key
        



# This functions rounds years 
def round(year, round_size):
    year = int(year)
    year = year - (year % round_size)     
    return str(year)
  
        

    
# This function cleans years from other non numeric characters    
def just_numbers(string): 
   
    match = re.search(r'[0-9]+', string)
    if match is not None:
        return match.group(0)
    else:
        return None




# This function is the heart of the country - year search
# It returns a country and a year from one fact 
def get_info_from_type(fact_json, info_type):

    country = None
    year = None    
    
    if "place" in fact_json:
        place = fact_json.get("place")
        
        if info_type == INFO_TYPE.NORMALIZED:
            if INFO_TYPE.NORMALIZED in place:
                normalized = place.get(INFO_TYPE.NORMALIZED)
                for normal in normalized:
                    if "value" in normal:
                        value = normal.get("value")
                        country = extract_country(value)
                        break
                        
        elif info_type == INFO_TYPE.FORMAL:
            if INFO_TYPE.FORMAL in place:
                formal = place.get(INFO_TYPE.FORMAL)
                country = extract_country(formal)
        
        elif info_type == INFO_TYPE.ORIGINAL:
            if INFO_TYPE.ORIGINAL in place:
                original = place.get(INFO_TYPE.ORIGINAL)
                country = extract_country(original)
          

    if "date" in fact_json:
        date = fact_json.get("date")
        
        if info_type == INFO_TYPE.NORMALIZED:
            if INFO_TYPE.NORMALIZED in date:
                for normalized in date.get(INFO_TYPE.NORMALIZED):
                    if 'value' in normalized:
                        normal = normalized.get("value")
                        array = normal.split(' ')
                        noisy_year = array[len(array) - 1]
                        noisy_year = just_numbers(noisy_year)
                        if noisy_year is not None and len(noisy_year) > 2 and int(noisy_year) > 100:
                            year = int(noisy_year)
                            break
                    
        elif info_type == INFO_TYPE.FORMAL:
            if INFO_TYPE.FORMAL in date:
                formal = date.get(INFO_TYPE.FORMAL)
                noisy_year = just_numbers(formal)
                if noisy_year is not None and len(noisy_year) > 2 and int(noisy_year) > 100:
                    year = int(noisy_year)
                 
        elif info_type == INFO_TYPE.ORIGINAL:
            if INFO_TYPE.ORIGINAL in date:
                original = date.get(INFO_TYPE.ORIGINAL)
                words = original.split(" ")
                if len(words) > 2:
                    noisy_year = words[len(words) - 1]
                    noisy_year = just_numbers(noisy_year)
                    if noisy_year is not None and len(noisy_year) > 2 and int(noisy_year) > 100:
                        year = int(noisy_year)
    
    return country, year
   
   
   
   
# returns the json person from the specified ID
def get_person(person_id, persons):
    
    if persons is not None:
        filteredPersons = [person for person in persons if person.get('id') == person_id]
        if len(filteredPersons) > 0:
            return filteredPersons[0]
   
    return None
         
    
    

# returns a list of facts depending on the fact_type  
def get_fact_list(facts, fact_type):
    
    if facts is not None:
        filteredFacts = [fact for fact in facts if fact.get('type') == fact_type]
        if len(filteredFacts) > 0:
            return filteredFacts
    
    return None
    
   
    
    
# This functions tries to return two lists. One of countries and another one of years 
# by first searching at normalized data, then formal data, and finally, original data    
def get_info_from_fact_type(facts_json, fact_type):
   
    countries = list()
    years = list()
    filtered_facts = get_fact_list(facts_json, fact_type)
    if filtered_facts is not None:
        
        for fact_json in filtered_facts: 
                
            country, year = get_info_from_type(fact_json, INFO_TYPE.NORMALIZED)
            if country is not None:
                countries.append(country)
            if year is not None:
                years.append(year)
                        
            country, year = get_info_from_type(fact_json, INFO_TYPE.FORMAL)
            if country is not None:
                countries.append(country)
            if year is not None:
                years.append(year)
                            
            country, year = get_info_from_type(fact_json, INFO_TYPE.ORIGINAL)
            if country is not None:
                countries.append(country)
            if year is not None:
                years.append(year)

    return countries, years




# This function tries to collect the person's info from the data contained in 
# the principal json person
def get_info_from_principal_person(person_json):
    
    person = Person()
    
    if "facts" in person_json:
        facts_json = person_json.get("facts")
        
        countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.BIRTH_FACT)
        if len(countries) > 0:
            # first entry is the best that it found
            # remember that get_info_from_fact_type looks first at normalized data
            # second at formal data, and finally at original, so entry zero is the
            # best it found
            person.birth_country = countries[0] 
        if len(years) > 0:
            person.birth_year = years[0]                
        
        # If getting birth_country or birth_year from birth fact failed, then try christening fact
        if person.birth_country is None or person.birth_year is None:
            countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.CHRISTENING_FACT)
            if len(countries) > 0 and person.birth_country is None:
                person.birth_country = countries[0]
            if len(years) > 0 and person.birth_year is None:
                person.birth_year = years[0]            
        
        # If getting birth_country or birth_year from christening fact failed, then try residence fact
        if person.birth_country is None or person.birth_year is None:
            countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.RESIDENCE_FACT)
            if len(countries) > 0 and person.birth_country is None:
                person.birth_country = countries[0]
            if len(years) > 0 and person.birth_year is None:
                person.birth_year = years[0] 
                     
         # Now we are not looking for birth information. This section is looking for death data
        countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.DEATH_FACT)
        if len(countries) > 0:
            person.death_country = countries[0]
        if len(years) > 0:
            person.death_year = years[0]        
        
        # If getting death_country or death_year from death fact failed, then try burial fact
        if person.death_country is None or person.death_year is None:
            death_country, death_year = get_info_from_fact_type(facts_json, FACT_TYPES.BURIAL_FACT)
            if len(countries) > 0 and person.death_country is None:
                person.death_country = countries[0]
            if len(years) > 0 and person.death_year is None:
                person.death_year = years[0]
        
    return person
            
             
                  

# This function returns a list of ids from people related to the original person.
# The ids will be used to get the json object for that related person and see if
# we can extract countries and years
def get_person_ids_by_relationship(person_id, relationships, relationship_type):
    
    if relationships is not None:
        
        filteredRelationships = [relationship for relationship in relationships if relationship.get("type") == relationship_type]
        if filteredRelationships is not None:
            
            ids = list()
            
            for relationship in filteredRelationships:
                person1_id = relationship.get('person1').get('resource')[1:]
                person2_id = relationship.get('person2').get('resource')[1:]
                
                if person_id == person1_id:
                    ids.append(person2_id)
                elif person_id == person2_id:
                    ids.append(person1_id)
                                     
            if len(ids) > 0:
                return ids;
        
    return None
    
    
    

# This function tries to get countries and years from related people depending on the relationship type
# For example if the relationship is child then it will look for all  the ids of people of type child
# Then, it grab the json object for all child ids
# For every child it will collect all the birth years it can find, all the birth countries, all the death years
# and all the death countries, and it will store them in their respective lists. So, at the end we will have
# four lists made our of the information of all the children.
# Now we estimate the parent birth year by taking an avarage of the birth_years list and substracting 25 years from 
# the avarage of birth years. We do this because in avarage parents are born 25 years before their children
# then, to estimate the birth country, from the birth countries list we grab the country that appears more times.
# This is done becauuse we think that the majority of the children might be born in the parent's birth country
def get_info_from_related_persons(person, person_id, persons_json, relationships_json, relationship_type):
    
    related_persons_ids = get_person_ids_by_relationship(person_id, relationships_json, relationship_type);
    if related_persons_ids is not None:
        birth_countries = list()
        birth_years = list()
        death_countries = list()
        death_years = list()        
        
        for related_person_id in related_persons_ids:
            related_person_json = get_person(related_person_id, persons_json);
            if related_person_json is not None:
                if "facts" in related_person_json:
                    facts_json = related_person_json.get("facts")               
                
                    countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.BIRTH_FACT)
                    birth_countries.extend(countries)
                    birth_years.extend(years)
                    
                    countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.CHRISTENING_FACT)
                    birth_countries.extend(countries)
                    birth_years.extend(years) 
                    
                    countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.RESIDENCE_FACT)
                    birth_countries.extend(countries)
                    # using residence fact to estimate birth place, but not birth year
                    #birth_years.extend(years) 
                        
                    countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.DEATH_FACT)
                    death_countries.extend(countries)
                    death_years.extend(years)
                    
                    countries, years = get_info_from_fact_type(facts_json, FACT_TYPES.BURIAL_FACT)
                    death_countries.extend(countries)
                    death_years.extend(years)
                   
        if person.birth_country is None and len(birth_countries) > 0:
            person.birth_country = argmax(birth_countries)     
        if person.birth_year is None and len(birth_years) > 0:
            avarage = sum(birth_years)/len(birth_years) - 25 if relationship_type == RELATIONSHIP_TYPES.CHILD else 0
            person.birth_year = avarage if avarage > 100 else None
        if person.death_country is None and len(death_countries) > 0:
            person.death_country = argmax(death_countries)            
        if person.death_year is None and len(death_years) > 0:
            avarage = sum(death_years)/len(death_years) - 25 if relationship_type == RELATIONSHIP_TYPES.CHILD else 0 
            person.death_year = avarage if avarage > 100 else None   
            
    return person





def get_info(person_id, persons_json, relationships_json):

   # First try get the info from the principal person json object
    person_json = get_person(person_id, persons_json)
    person = get_info_from_principal_person(person_json)
    
    # If getting info from the principal person fails, look at an avarage data of spouses (They are very probable, to be really close
    # in age and be born in the same country)
    if person.birth_country is None or person.birth_year is None or person.death_year is None or person.death_year is None:
        person = get_info_from_related_persons(person, person_id, persons_json, relationships_json, RELATIONSHIP_TYPES.MARRIAGE)
          
    if person.birth_country is None or person.birth_year is None or person.death_year is None or person.death_year is None:
        person = get_info_from_related_persons(person, person_id, persons_json, relationships_json, RELATIONSHIP_TYPES.COUPLE)    
    
    # if getting info from spouses fails, then look at the avarage data of children. year are substracted 25 years
    if person.birth_country is None or person.birth_year is None or person.death_year is None or person.death_year is None:
        person = get_info_from_related_persons(person, person_id, persons_json, relationships_json, RELATIONSHIP_TYPES.CHILD)
    
    # if at the end of the search we have a lonely year with no country,
    # we do not want to waste that year(Spark needs country - year pairs), 
    # so give it the same country from birth or death countries. 
    # Chances are that this person was born and died in the same country anyway..
    if person.birth_country is None and person.death_country is not None:
        person.birth_country = person.death_country
    elif person.death_country is None and person.birth_country is not None:
        person.death_country = person.birth_country
      
    return person 

 
   
   
#============================ Parser Main Function ============================




def get_person_info(line):

    #line = unidecode(line)
  
    # getting ready to extract important info from this person
    person_id = line[:23]
    data_json = json.loads(line[24:])
    persons_json = data_json.get('persons')
    relationships_json = data_json.get('relationships')
  
    # we get estimates of the birth country-year pair, and the death country-year pair
    # from the principal person or from related people
    person = get_info(person_id, persons_json, relationships_json)
        
    # we round years
    if person.birth_year is not None:
        person.birth_year = round(person.birth_year, BUCKET_SIZE)
    if person.death_year is not None:
        person.death_year = round(person.death_year, BUCKET_SIZE)
    
    # we make the output list that will be used by spark
    output = list()
    
    # check if the pair birth country-year is complete and add it to the output list
    if person.birth_country is not None and person.birth_year is not None:
        output.append(str(person.birth_country.encode("utf-8")) + "::" + str(person.birth_year))
    
    # checking that the pair death country-year is complete
    if person.death_country is not None and person.death_year is not None:
        # checking that death country and year is not exactly the same as the birth country-year pair
        # we do not want to count this person twice
        if person.birth_country != person.death_country and person.birth_year != person.death_year:
            output.append(str(person.death_country.encode("utf-8")) + "::" + str(person.death_year))
            
    if len(output) == 0:
        output.append("NO COUNTRY::NO YEAR")
    
    return output
    
    
    
    

#==============================  Spark  ===================================





conf = SparkConf()
# This lines lets spark overwrite output files
conf.set("spark.hadoop.validateOutputSpecs", "false")
sc = SparkContext(conf = conf)

home = os.getenv('HOME', None)
inputFolder = home + '/fsl_groups/fslg_familytree/compute/TreeData/'
outputFolder = home + '/compute/family_search/'

lines = sc.textFile(inputFolder + '*.json.gz')
persons = lines.flatMap(lambda line: get_person_info(line))
persons = persons.map(lambda x: (x, 1))
persons = persons.reduceByKey(lambda a, b: a+b)
persons = persons.sortByKey(True)
persons.saveAsTextFile(outputFolder)
