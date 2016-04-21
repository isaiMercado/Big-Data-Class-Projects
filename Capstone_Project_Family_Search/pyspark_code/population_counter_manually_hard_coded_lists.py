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
    
   
    
class COUNTRIES: 

    AMERICA = ['united states','barbuda','bahamas','barbados','belize','canada','costa rica','cuba','dominica','dominican republic','salvador','grenada',
                     'guatemala','haiti','honduras','jamaica','mexico','nicaragua','panama','nevis','lucia','grenadines','trinidad',
                     'argentina','bolivia','brazil','chile','colombia','ecuador','guyana','paraguay','peru','suriname','uruguay','venezuela']
                     
    EUROPE = ['albania','andorra','armenia','austria','azerbaijan','belarus','belgium','bosnia','bulgaria','croatia','cyprus',
                    'czech republic','denmark','estonia','finland','france','georgia','germany','greece','hungary','iceland','ireland',
                    'italy','kazakhstan','kosovo','latvia','liechtenstein','lithuania','luxembourg','macedonia','malta','moldova','monaco',
                    'montenegro','netherlands','norway','poland','portugal','romania','russia','marino','serbia','slovakia','slovenia',
                    'spain','sweden','switzerland','turkey','ukraine','united kingdom','vatican']
                    
    ASIA = ['afghanistan','armenia','azerbaijan','bahrain','bangladesh','bhutan','brunei','cambodia','china','cyprus',
                  'georgia','india','indonesia','iran','iraq','israel','japan','jordan','kazakhstan','kuwait',
                  'kyrgyzstan','laos','lebanon','malaysia','maldives','mongolia','myanmar','nepal','oman',
                  'pakistan','palestine','philippines','qatar','russia','saudi arabia','singapore','korea','lanka',
                  'syria','taiwan','tajikistan','thailand','timor-leste','turkey','turkmenistan','arab emirates','uzbekistan','vietnam','yemen']
                  
    AFRICA = ['algeria','angola','benin','botswana','burkina','burundi','cabo verde','cameroon','african republic','chad',
                    'comoros','congo','divoire','djibouti','egypt','guinea','eritrea','ethiopia','gabon','gambia','ghana','bissau',
                    'kenya','lesotho','liberia','libya','madagascar','malawi','mali','mauritania','mauritius','morocco',
                    'mozambique','namibia','niger','nigeria','rwanda','principe','senegal','seychelles','sierra leone','somalia',
                    'south africa','south sudan','sudan','swaziland','tanzania','togo','tunisia','uganda','zambia','zimbabwe']
                    
    OCEANIA = ['australia','fiji','kiribati','marshall','micronesia','nauru','zealand','palau','new guinea',
                     'samoa','solomon islands','tonga','tuvalu','vanuatu']
                     
                     
class PLACE(object):
    LIST = list()
    COUNTRY = ""


#--------------------------------------------AMERICA----------------------------------------------------


class BARBUDA(PLACE):
    LIST = ['antigua','barbuda','bird','bishop','blake','cinnamon','codrington','crump','dulcina','exchange','five','great bird',
    'green','guiana','hale gate','hawes','henry','johnson','kid','laviscounts','lobster','long','maid','moor','nanny','pelican',
    'prickly','rabbit','rat','red head','redonda','sandy','smith','sisters','vernon','wicked','york island']
    COUNTRY = "barbuda"
    

class BAHAMAS(PLACE):
    LIST = ['acklins','berry','bimini','black','exuma','cat','abaco','andros','eleuthera','freeport','bahama','crooked',
    'exuma','abaco','harbour','eleuthera','hope','inagua','long','mangrove cay','andros','mayaguana','moore','abaco','andros',
    'eleuthera','ragged','rum cay','san salvador','andros','eleuthera','spanish wells']
    COUNTRY = "bahamas"
    
    
class BARBADOS(PLACE):
    LIST = ['christ','saint andrew','saint george','saint james','saint john','saint joseph','saint lucy','saint michael',
    'saint peter','saint philip','saint thomas']
    COUNTRY = "barbados"
    
    
class BELIZE(PLACE):
    LIST = ['cayo','ignacio','orange','corozal','stann creek','dangriga','toledo']
    COUNTRY = "belize"
    
    
class CANADA(PLACE):
    LIST = ['ontario','toronto','quebec','montreal','scotia','halifax','brunswick','fredericton','manitoba','winnipeg','columbiav','victoria',
    'vancouver','prince edward','charlottetown','saskatchewan','saskatoon','alberta','edmonton','calgary','newfoundland','labrador','john']
    COUNTRY = "canada"
    
    
class COSTA_RICA(PLACE):
    LIST = ['alajuela','cartago','guanacaste','liberia','heredia','limon','puntarenas','san jose']
    COUNTRY = "costa rica"
    
   
class CUBA(PLACE):
    LIST = ['pinar','artemisa','habana','mayabeque','matanzas','cienfuegos','villa clara','sancti','ciego de avila','camaguey','tunas',
    'granma','holguin','santiago','guantanamo','juventud','pinos']
    COUNTRY = "cuba"
    
    
class DOMINICA(PLACE):
    LIST = ['andrew parish','david parish','george parish','john parish','joseph parish','luke parish','mark parish','patrick parish','paul parish','peter parish']
    COUNTRY = "dominica"
    
    
class DOMINICAN(PLACE):
    LIST = ['azua','baoruco','neiba','barahona','dajabon','distrito nacional','santo domingo','duarte','macoris','seibo','elias pina','comendador',
    'espaillat','moca','hato','mirabal','salcedo','independencia','jimani','altagracia','higuey','romana','concepcion de la vega','trinidad sanchez','nagua',
    'nouel','bonao','cristi','monte plata','pedernales','bani','puerto plata','samana','cristobal','ocoa','maguana','macoris','cotui','santiago',
    'sabaneta','santo domingo','valverde','mao']
    COUNTRY = "dominican republic"
    
    
class SALVADOR(PLACE):
    LIST = ['ahuachapan','santa ana','sonsonate','libertad','tecla','chalatenango','cuscatlan','cojutepeque','la paz','zacatecoluca',
    'cabanas','sensuntepeque','san vicente','usulutan','miguel','morazan','gotera','union']
    COUNTRY = "salvador"


class GUATEMALA(PLACE):
    LIST = ['peten','flores','huehuetenango','quiche','verapaz','coban','izabal','barrios','san marcos','quetzaltenango','totonicapan','solola',
    'chimaltenango','sacatepequez','antigua','salama','progreso','guastatoya','sanarate','jalapa','zacapa','chiquimula','retalhuleu','suchitepequez',
    'mazatenango','escuintla','santa rosa','cuilapa','jutiapa']
    COUNTRY = "guatemala"
    
    
class HAITI(PLACE):
    LIST = ['artibonito','gonaives','hincha','grandanse','jeremie','nippes','miragoane','cabo','fuerte libertad','paix','principe','cayes','jacmel']
    COUNTRY = "haiti"
    
    
class HONDURAS(PLACE):
    LIST = ['atlantida','choluteca','colon','comayagua','copan','cortes','paraiso','morazan','gracias','intibuca','bahia',
    'la paz','lempira','ocotepeque','olancho','barbara','valle','yoro']
    COUNTRY = "honduras"
    
    
class JAMAICA(PLACE):
    LIST = ['hanover','lucea','clarendon','may pen','kingston','elizabeth','black river','manchester','mandeville','portland','port antonio',
    'montego bay','ann','saint andrew','tree','trelawny','falmouth','catherine','spanish town','saint thomas','morant bay','westmoreland',
    'savanna','la mar','maria']
    COUNTRY = "jamaica"
    
    
class NICARAGUA(PLACE):
    LIST = ['autonomista','jinotega','nueva segovia','madriz','esteli','matagalpa','boaco','chontales','chinandega','leon','masaya',
    'managua','grenada','carazo','rivas','rio san juan']
    COUNTRY = "nicaragua"
    
    
class PANAMA(PLACE):
    LIST = ['toro','chiriqui','cocle','colon','darien','herrera','santos','veraguas','chorrera','regions','embera','guna',
    'yala','ngobe','bugle','comarca','kuna','madugandi','wargandi']
    COUNTRY = "panama"
    
    
class NEVIS(PLACE):
    LIST = ['nichola','anne','sandy','basseterre','gingerland','windward','capesterre','figtree','mary cayon','capisterre',
    'charlestown','basseterre','lowland','saint thomas','trinity','palmetto','kitts']
    COUNTRY = "nevis"
    
    
class LUCIA(PLACE):
    LIST = ['anse','la raye','canaries','castries','choiseul','dauphin','dennery','gros','laborie','micoud','praslin','soufriere','vieux fort']
    COUNTRY = "lucia"
    
    
class GRENADINES(PLACE):
    LIST = ['charlotte','georgetown','grenadines','elizabeth','saint andrew','layou','saint david','chateaubelair','saint george',
    'kingstown','saint patrick','barrouallie','parishes']
    COUNTRY = "grenadines"
    
    
class TRINIDAD(PLACE):
    LIST = ['arima','chaguanas','caroni','couva','tabaquite','talparo','victoria','diego martin','petit valley','tobago','roxborough',
    'mayaro','rio claro','nariva','penal','debe','penal','saint patrick','fortin','port of spain','princes town','san fernando','sangre grande',
    'saint andrew','saint david','san juan','laventille','siparia','tunapuna','piarco','scarborough']
    COUNTRY = "trinidad"
    
    
class ARGENTINA(PLACE):
    LIST = ['buenos aires','la plata','catamarca','san fernando','chaco','chubut','rawson','cordoba','corrientes','entre rios','parana','formosa',
    'jujuy','la pampa','santa rosa','la rioja','mendoza','misiones','posadas','neuquen','rio negro','viedma','salta','san juan','san luis','santa cruz',
    'rio gallegos','santa fe','estero','tierra del fuego','ushuaia','tucuman']
    COUNTRY = "argentina"
    
    
class BOLIVIA(PLACE):
    LIST = ['pando','cobija','la paz','beni','trinidad','oruro','cochabamba','sierra','chuquisaca	sucre','tarija']
    COUNTRY = "bolivia"
    
    
class USA(PLACE):
    LIST = ['alabama','alaska','arizona','arkansas','california','colorado','connecticut','delaware','florida','georgia','hawaii','idaho',
    'illinois','illinois','indiana','iowa','kansas','kentucky','louisiana','maine','maryland','massachusetts','michigan','minnesota','mississippi',
    'missouri','montana','nebraska','nevada','hampshire','jersey','new mexico','york','ohio','oklahoma','oregon','pennsylvania','rhode island',
    'carolina','dakota','tennessee','texas','utah','vermont','virginia','washington','virginia','wisconsin','wyoming','american samoa','baker island',
    'guam','howland island','jarvis island','johnston atoll','kingman reef','midway islands','navassa island','mariana islands','palmyra atoll',
    'puerto rico','virgin islands', 'louisianna','tennesse','u.s.a.',' usa ',' us ',' al ',' ak ',' az ',' ar ',' ca ',' co ',' ct ',' de ',' fl ',' ga ',
    ' hi ',' id ',' il ',' in ',' ia ',' ks ',' ky ',' la ',' me ',' md ',' ma ',' mi ',' mn ',' ms ',' mo ',' mt ',' ne ',' nv ',' nh ',' nj ',
    ' nm ',' ny ',' nc ',' nd ',' oh ',' ok ',' or ',' pa ',' ri ',' sc ',' sd ',' tn ',' tx ',' ut ',' vt ',' va ',' wa ',' wv ',' wi ',' wy ',
    ' as ',' dc ',' fm ',' gu ',' mh ',' mp ',' pw ',' pr ',' vi ','n.y.','america','massechusetts','penn.','u.s.','kentucky','maryland','dallas']         
    COUNTRY = "united states"

           
class MEXICO(PLACE):
    LIST = ['chihuahua','sonora','coahuila','durango','oaxaca','tamaulipas','jalisco','zacatecas','chiapas','veracruz','baja california','nuevo leon',
    'guerrero','san luis potosi','michoacan','campeche','sinaloa','quintana roo','yucatan','puebla','guanajuato','nayarit',
    'tabasco','mexico','hidalgo','queretaro','colima','aguascalientes','morelos','tlaxcala','ma(c)xico',' mex ',' mex. ','tampico','tequisquiapan','yautepec']
    COUNTRY = "mexico"

    
class BRAZIL(PLACE):
    LIST = ['acre','rio branco','alagoas','maceio','amapa','macapa','amazonas','manaus','bahia','ceara','fortaleza','brasilia','espirito santo',
    'vitoria','goias','goiania','maranhao','mato grosso','cuiaba','do sul','campo grande','minas gerais','belo horizonte','para','belem','paraiba',
    'joao pessoa','parana','pernambuco','recife','piaui','janeiro','rio grande','alegre','rondonia','velho',
    'roraima','catarina','florianopolis','paulo','sergipe','aracaju','tocantins','palmas']
    COUNTRY = "brazil"
    
    
class CHILE(PLACE):
    LIST = ['arica','parinacota','iquique','tamarugal','antofagasta','el loa','tocopilla','copiapo','huasco',
    'chanaral','elqui','limari','choapa','pascua','andes','marga','petorca','quillota','san antonio','aconcagua','valparaiso','cachapoal',
    'colchagua','cardenal caro','talca','linares','curico','cauquenes','concepcion','nuble','biobio','arauco','cautin','malleco','valdivia',
    'ranco','llanquihue','osorno','chiloe','palena','coihaique','aisen','carrera','capitan prat','magallanes','ultima','fuego','chilena',
    'santiago','cordillera','maipo','talagante','melipilla','chacabuco','arica','putre','iquique','almonte','antofagasta',
    'calama','tocopilla','copiapo','vallenar','chanaral','serena','ovalle','illapel','hanga','quilpue','la ligua','quillota',
    'felipe','valparaiso','rancagua','fernando','pichilemu','talca','linares','curico','cauquenes','concepcion','chillan','angeles',
    'lebu','temuco','angol','valdivia','union','puerto montt','osorno','castro','futaleufu','coihaique','puerto aisen','chico',
    'cochrane','arenas','natales','porvenir','williams','santiago','puente alto','bernardo','talagante','melipilla','colina']
    COUNTRY = "chile"
    
    
class COLOMBIA(PLACE):
    LIST = ['bogota','amazonas','antioquia','arauca','atlantico','bolivar','boyaca','caldas','caqueta','casanare','cauca','cesar',
    'choco','cordoba','cundinamarca','guainia','guaviare','huila','guajira','magdalena','meta','narino','santander','putumayo','quindio',
    'risaralda','san andres','providencia','santander','sucre','tolima','valle del cauca','vaupes','vichada','leticia',
    'medellin','arauca','barranquilla','cartagena','tunja','manizales','florencia','yopal','popayan','valledupar','quibdo',
    'monteria','bogota','inirida','guaviare','neiva','riohacha','santa marta','villavicencio','pasto','cucuta','mocoa','armenia',
    'pereira','san andres','bucaramanga','sincelejo','ibague','cali','mitu','carreno']
    COUNTRY = 'colombia'
    
    
class ECUADOR(PLACE):
    LIST = ['azuay','bolivar','canar','carchi','chimborazo','cotopaxi','el oro','esmeraldas','galapagos','guayas','imbabura','loja',
    'los rios','manabi','morona','napo','orellana','pastaza','pichincha','santa elena','tsachilas','sucumbios','tungurahua',
    'chinchipe','cuenca','guaranda','azogues','tulcan','riobamba','latacunga','machala','esmeraldas','baquerizo','guayaquil',
    'ibarra','loja','babahoyo','portoviejo','macas','tena','orellana','puyo','quito','santa elena','santo domingo','nueva loja','ambato','zamora']
    COUNTRY = "ecuador"
    
    
class GUYANA(PLACE):
    LIST = ['barima','waini','pomeroon','supenaam','essequibo','demerara','mahaica','berbice','corentyne','cuyuni','mazaruni','potaro',
    'siparuni','takutu','essequibo','demerara','berbice','guyana']
    COUNTRY = "guyana"
    
    
class PARAGUAY(PLACE):
    LIST = ['concepcion','san pedro','cordillera','guaira','caaguazu','caazapa','itapua','misiones','paraguari','alto parana',
    'central','neembucu','amambay','canindeyu','presidente hayes','alto paraguay','boqueron','paraguay','asuncion',
    'concepcion','caacupe','villarrica','oviedo','caazapa','encarnacion','bautista','paraguari','ciudad del este','aregua','pilar',
    'caballero','guaira','hayes','fuerte olimpo','filadelfia','asuncion']
    COUNTRY = "paraguay"
    
    
class PERU(PLACE):
    LIST = ['amazonas','ancash','apurimac','arequipa','ayacucho','cajamarca','callao','cuzco','huancavelica','huanuco','ica','junin','la libertad',
    'lambayeque','lima','loreto','madre de dios','moquegua','pasco','piura','puno','san martin','tacna','tumbes','ucayali','province','lima']
    COUNTRY = "peru"
    
    
class SURINAME(PLACE):
    LIST = ['brokopondo','commewijne','coronie','marowijne','nickerie','para','paramaribo','saramacca','sipaliwini','wanica','nieuw','amsterdam',
    'totness','albina','nickerie','onverwacht','paramaribo','groningen','none','lelydorp','paramaribo']
    COUNTRY = "suriname"
    
    
class URUGUAY(PLACE):
    LIST = ['artigas','canelones','cerro largo','colonia','durazno','flores','florida','lavalleja','maldonado','montevideo','paysandu','rio negro',
    'san jose','soriano','tacuarembo','treinta y tres','melo','sacramento','durazno','trinidad','florida','minas','maldonado','montevideo','paysandu',
    'fray bentos','rivera','rocha','salto','san jose de mayo','mercedes','tacuarembo']
    COUNTRY = "uruguay"
    
    
class VENEZUELA(PLACE):
    LIST = ['amazonas','anzoategui','apure','aragua','barinas','bolivar','carabobo','cojedes','delta amacuro','falcon','guarico',
    'lara','merida','miranda','monagas','nueva esparta','portuguesa','sucre','tachira','trujillo','vargas','yaracuy','zulia','puerto ayacucho',
    'barcelona','san fernando de apure','maracay','barinas','ciudad bolivar','valencia','san carlos','tucupita','coro','san juan de los morros',
    'barquisimeto','merida','los teques','maturin','la asuncion','guanare','cumana','san cristobal','trujillo','la guaira','san felipe','maracaibo']
    COUNTRY = "venezuela"


#-----------------------------------------------------------EUROPE---------------------------------------------------
          

class ALBANIA(PLACE):
    LIST = ['tirana','durres','vlore','elbasan','shkoder','korce','fier','kamez','berat','lushnje','sarande','paskuqan','kavaje','pogradec','gjirokaster']
    COUNTRY = "albania"
    
    
class ANDORRA(PLACE):
    LIST = ['andorra','la vella','canillo','encamp','escaldes','engordany','massana','ordino','julia','loria']
    COUNTRY = "andorra"
    
    
class ARMENIA(PLACE):
    LIST = ['aragatsotn','ararat','armavir','gegharkunik','kotayk','lori','shirak','syunik','tavush','vayots dzor','yerevan','ashtarak','artashat',
    'armavir','gavar','hrazdan','vanadzor','gyumri','kapan','ijevan','yeghegnadzor']
    COUNTRY = "armenia"
    
    
class AUSTRIA(PLACE):
    LIST = ['vienna','wien','niederosterreich','oberosterreich','styria','steiermark','tyrol','tirol','carinthia','karnten','salzburg',
    'vorarlberg','burgenland','sankt polten','linz','graz','innsbruck','klagenfurt','salzburg','bregenz','eisenstadt']
    COUNTRY = "austria"


class GERMANY(PLACE):
    LIST = ['bavaria','baden','wurttemberg','rhine','westphalia','hesse','saxony','rhineland','palatinate','thuringia','brandenburg',
    'saxony-anhalt','mecklenburg','pomerania','schleswig','holstein','saarland','bremen','berlin','hamburg']
    COUNTRY = "germany"

               
class ITALY(PLACE):
    LIST = ['abruzzo','aquila','aosta','apulia','puglia','bari','basilicata','potenza','calabria','catanzaro','campania','naples',
    'romagna','bologna','venezia','trieste','lazio','rome','liguria','genoa','lombardia','milan','marche','ancona','molise','campobasso',
    'piemonte','turin','sardegna','cagliari','sicilia','palermo','trentino','adige','toscana','florence','umbria','perugia','veneto','venice']
    COUNTRY = "italy"
    
    
class AZERBAIJAN(PLACE):
    LIST = ['absheron','ganja','qazakh','shaki','zaqatala','lankaran','quba','khachmaz','aran','yukhari','karabakh',
    'kalbajar','lachin','daglig','shirvan','nakhchivan']
    COUNTRY = "azerbaijan"
    
    
class BELARUS(PLACE):
    LIST = ['minsk','gomel','mogilev','vitebsk','grodno','brest']
    COUNTRY = "belarus"
    
    
class BELGIUM(PLACE):
    LIST = ['antwerp','east flanders','flemish brabant','limburg','west flanders','hainaut','liege','luxembourg','namur','walloon brabant','antwerpen',
    'vlaanderen','vlaams-brabant','limburg','henegouwen','luik','luxemburg','namen','waals-brabant','anvers','brabant flamand','limbourg',
    'hainaut','liege','luxembourg','namur','brabant wallon','antwerpen','ostflandern','brabant','limburg','westflandern','hennegau','luttich',
    'luxemburg','namur','antwerpen','gent','leuven','hasselt','brugge','mons','liege','arlon','namur','wavre']
    COUNTRY = "belgium"
    
    
class BOSNIA(PLACE):
    LIST = ['sarajevo','banja luka','tuzla','zenica','bijeljina','mostar','prijedor','brcko','doboj','cazin','zvornik',
    'zivinice','bihac','travnik','gradiska','gracanica','sanski most','lukavac','tesanj','velika kladusa']
    COUNTRY = "bosnia"
    
    
class BULGARIA(PLACE):
    LIST = ['blagoevgrad','burgas','dobrich','gabrovo','haskovo','kardzhali','kyustendil','lovech','montana','pazardzhik',
    'pernik','pleven','plovdiv','razgrad','ruse','shumen','silistra','sliven','smolyan','sofia province','stara zagora',
    'targovishte','varna','veliko tarnovo','vidin','vratsa','yambol']
    COUNTRY = "bulgaria"
    
    
class CROATIA(PLACE):
    LIST = ['baranja','bilogora','bribir','sidraga','bribir','cetina','dubrava','gora','zagorje','hum','krbava-psat','lasva-glaz','lasva-pliva',
    'lika-gacka','livac-zapolje','modrus','pliva-rama','pokupje','posavje','prigorje','sana-luka','usora-soli','vinodol-podgorje','vrhbosna',
    'vuka','zagorje','sidraga','ravni kotari']
    COUNTRY = "croatia"
    

class CYPRUS(PLACE):
    LIST = ['famagusta','gazimagusa','kyrenia','girne','larnaca','larnaka','limassol','limasol','leymosun','nicosia','lefkosa','paphos','baf','gazibaf']
    COUNTRY = "cyprus"
    
    
class CZECH(PLACE):
    LIST = ['prague','central bohemia','south bohemia','plzen','karlovy vary','usti nad labem','liberec','hradec kralove','pardubice',
    'olomouc','moravia-silesia','south moravia','zlin','vysocina','hlavni mesto','stredocesky','jihocesky',
    'plzensky','karlovarsky','ustecky','liberecky','kralovehradecky','pardubicky','olomoucky','moravskoslezsky','jihomoravsky','zlinsky','kraj vysocina',
    'prague','ceske budejovice','plzen','karlovy vary','usti nad labem','liberec','hradec kralove','pardubice','olomouc','ostrava',
    'brno','zlin','jihlava','prague']
    COUNTRY = "czech republic"
    

class DENMARK(PLACE):
    LIST = ['hovedstaden','midtjylland','nordjylland','sjaelland','syddanmark','danmark','copenhagen','aarhus','aalborg','roskilde','odense','copenhagen']
    COUNTRY = "denmark"
         
         
class ESTONIA(PLACE):
    LIST = ['harju','hiiu','ida-viru','jogeva','jarva','laane','laane-viru','polva','parnu','rapla','saare','tartu','valga',
    'viljandi','voru','tallinn','kardla','johvi','jogeva','paide','haapsalu','rakvere','polva','parnu','rapla','kuressaare']
    COUNTRY = "estonia"
    
    
class FINLAND(PLACE):
    LIST = ['helsinki','espoo','tampere','vantaa','turku','oulu','lahti','kuopio','jyvaskyla','pori','lappeenranta','rovaniemi','joensuu','vaasa','kotka']
    COUNTRY = "finland"
              

class FRANCE(PLACE):
    LIST = ['alsace','champagne','ardenne','lorraine','ardennes','aube','marne','haute-marne','meurthe','moselle','meuse','moselle','bas-rhin',
    'haut-rhin','vosges','aquitaine','limousin','poitou','charentes','charente','charente-maritime','correze','creuse',
    'dordogne','gironde','landes','garonne','pyrenees','atlantiques','deux-sevres','vienne','haute','vienne','auvergne','rhone',
    'allier','ardeche','cantal','drome','isere','loire','haute','loire','puy-de-dome','rhone','savoie','haute-savoie','bourgogne','franche','comte',
    'cote-dor','doubs','nievre','haute','saone','loire','yonne','belfort','brittany','bretagne','cotes','armor','finistere','vilaine','morbihan',
    'eure-et-loir','indre','loir-et-cher','loiret','corsica','corse','paris','seine','marne','yvelines','essonne','languedoc',
    'roussillon','pyrenees','ariege','aveyron','garonne','herault','lozere','hautes','pyrenees','calais','picardy','aisne',
    'oise','somme','normandy','normandie','calvados','manche','maritime','mayenne','sarthe','vendee','alpes','bouches',
    'rhone','vaucluse','guadeloupe','martinique','guiana','reunion','mayotte','pierre','miquelon','barthelemy','martin',
    'wallis','futuna','french polynesia','generis','caledonia','clipperton']
    COUNTRY = "france"
    
    
class GEORGIA(PLACE):
    LIST = ['abkhazia','sukhumi','samegrelo-zemo','svaneti','zugdidi','guria','ozurgeti','adjara','batumi','racha','lechkhumi','kvemo',
    'svaneti','ambrolauri','imereti','kutaisi','samtskhe','javakheti','akhaltsikhe','shida','kartli','mtskheta','mtianeti',
    'mtskheta','kvemo','kartli','rustavi','kakheti','telavi','tbilisi']
    COUNTRY = "georgia"
        
    
class GREECE(PLACE):
    LIST = ['attica','macedonia','crete','thrace','epirus','ionian','aegean','peloponnese','thessaly','athens','lamia','thessaloniki','heraklion',
    'komotini','ioannina','corfu','mytilene','tripoli','ermoupoli','larissa','patras','kozani']
    COUNTRY = "greece"
        
    
class HUNGARY(PLACE):
    LIST = ['budapest','kiskun','baranya','bekes','borsod','abauj','zemplen','csongrad','fejer','moson','sopron','hajdu','bihar','heves','nagykun','szolnok',
    'komarom','esztergom','nograd','pest','somogy','szabolcs','szatmar','bereg','tolna','veszprem','kecskemet','bekescsaba','miskolc','szeged',
    'szekesfehervar','debrecen','szolnok','tatabanya','salgotarjan','budapest','kaposvar','nyiregyhaza','szekszard','szombathely','veszprem','zalaegerszeg']
    COUNTRY = "hungary"
        
    
class ICELAND(PLACE):
    LIST = ['arnessysla','bardastrandarsysla','hunavatnssysla','austur','skaftafellssysla','borgarfjardarsysla','dalasysla','eyjafjardarsysla','gullbringusysla',
    'kjosarsysla','myrasysla','nordur','isafjardarsysla','mulasysla','thingeyjarsysla','rangarvallasysla','skagafjardarsysla','snaefellsnes',
    'hnappadalssysla','strandasysla','sudur','mulasysla','thingeyjarsysla','bardastrandarsysla','vestur','hunavatnssysla','isafjardarsysla',
    'skaftafellssysla','akranes','akureyri','bolungarvik','dalvik','eskifjordur','gardabaer','grindavik','hafnarfjordur','husavik','isafjordur',
    'keflavik','kopavogur','neskaupstadur','olafsfjordur','olafsvik','reykjavik','saudarkrokur','selfoss','seltjarnarnes','seydisfjordur',
    'siglufjordur','vestmannaeyjar']
    COUNTRY = "iceland"
        
    
class IRELAND(PLACE):
    LIST = ['antrim','armagh','carlow','cavan','clare','cork','donegal','down','dublin','fermanagh','galway','kerry','kildare','kilkenny','laois','leitrim',
    'limerick','londonderry','longford','louth','meath','monaghan','offaly','roscommon','sligo','tipperary','tyrone','waterford','westmeath',
    'wexford','wicklow','laoghaire','rathdown','fingal','dublin','aontroim','mhacha','ceatharlach','cabhan','chabhain','chlair','corcaigh',
    'chorcai','ngall','cliath','manach','gaillimh','ciarrai','chiarrai','chainnigh','laois','laoise','liatroim','liatroma','luimneach','luimnigh',
    'doire','dhoire','longfort','longfoirt','maigh','muineachan','mhuineachain','fhaili','comain','sligeach','shligigh','tiobraid','thiobraid',
    'eoghain','lairge','iarmhi','hiarmhi','garman','mhantain','laoghaire','cliath']
    COUNTRY = "ireland"
        
    
class KAZAKHSTAN(PLACE):
    LIST = ['akmola','aktobe','almaty','astana','atyrau','baikonur','jambyl','karaganda','kostanay','kyzylorda','mangystau','pavlodar','aqmola','aqtobe',
    'almati','almati','astana','atiraw','bayqonir','qazaqstan','jambil','qaragandi','qostanay','qizilorda','mangistaw','soltustik','qazaqstan',
    'pavlodar','ontustik','batis','akmolinskaya','aktyubinskaya','gorod','almatinskaya','gorod','astana','atyrauskaya','gorod',
    'baykonur','vostochno','kazakhstanskaya','zhambylskaya','karagandinskaya','kostanayskaya','kyzylordinskaya','mangystauskaya','severo',
    'kazakhstanskaya','pavlodarskaya','yuzhno','kazakhstanskaya','zapadno','kazakhstanskaya']
    COUNTRY = "kazakhstan"
        
    
class KOSOVO(PLACE):
    LIST = ['ferizaj','hani i elezit','kacanik','stimlje','shtime','strpce','shterpce','decan','gjakova','junik','rahovec','gjilan',
    'kamenica','klokot','partesh','ranilug','vitina','leposavic','mitrovica','north mitrovica','skenderaj','vushtrri','zubin potok',
    'zvecan','peje/pec','istok','klina','glogovac','drenas','gracanica','kosovo','polje','fushe','kosove','lipljan',
    'novo brdo','obilic','podujevo','pristina','dragas','malisheva','mamusha','prizren','suva reka','suhareke']
    COUNTRY = "kosovo"
        
    
class LATVIA(PLACE):
    LIST = ['aglona','aizkraukle','aizpute','akniste','aloja','alsunga','aluksne','amata','ape','auce','adazi','babite','baldone',
    'baltinava','balvi','bauska','beverina','broceni','burtnieki','carnikava','cesis','cesvaine','cibla','dagda','daugavpils','dobele',
    'dundaga','durbe','engure','ergli','garkalne','grobina','gulbene','iecava','ikskile','incukalns','ilukste','jaunjelgava','jaunpiebalga',
    'jaunpils','jekabpils','jelgava','kandava','karsava','koceni','koknese','kraslava','krimulda','krustpils','kuldiga','kegums','kekava',
    'lielvarde','ligatne','limbazi','livani','lubana','ludza','madona','malpils','marupe','mazsalaca','mersrags','naukseni','nereta','nica',
    'ogre','olaine','ozolnieki','pargauja','pavilosta','plavinas','preili','priekule','priekuli','rauna','rezekne','riebini','roja','ropazi',
    'rucava','rugaji','rundale','rujiena','salacgriva','sala','salaspils','saldus','saulkrasti','seja','sigulda','skriveri','skrunda','smiltene',
    'stopini','strenci','talsi','tervete','tukums','vainode','valka','varaklani','varkava','vecpiebalga','vecumnieki','ventspils','viesite','vilaka',
    'vilani','zilupe']
    COUNTRY = "latvia"
        
    
class LIECHTENSTEIN(PLACE):
    LIST = ['ruggell','schellenberg','gamprin','bendern','eschen','nendeln','mauren','schaanwald','schaan','muhleholz','planken','hinterschellenberg','vaduz',
    'ebenholz','triesenberg','gaflei','malbun,','masescha','rotenboden,','samina','silum','steg','sucka','wangerberg','triesen','balzers','mals']
    COUNTRY = "liechtenstein"
    

class LITHUANIA(PLACE):
    LIST = ['alytus','birzai','pasvalys','kaunas','kedainiai','klaipeda','kretinga','marijampole','mazeikiai','pagegiai','panevezys','raseiniai',
    'rokiskis','sejny','sakiai','siauliai','silute','taurage','telsiai','trakai','ukmerge','utena','vilkaviskis','zarasai']
    COUNTRY = "lithuania"  
    
    
class LUXEMBOURG(PLACE):
    LIST = ['arsdorf','asselborn','bascharage','bastendorf','bigonville','boevange','burmerange','clemency','consthum','eich','ermsdorf',
    'eschweiler','folschette','fouhren','hachiville','hamm','harlange','heiderscheid','heinerscheid','hollerich','hoscheid','hosingen',
    'kautenbach','mecher','medernach','munshausen','neunhausen','oberpallen','oberwampach','perle','rodenbourg','rollingergrund','wellenstein',
    'wilwerwiltz','diekirch','differdange','dudelange','echternach','esch-sur-alzette','ettelbruck','grevenmacher','luxembourg','remich',
    'rumelange','vianden','wiltz']
    COUNTRY = "luxembourg" 
        
    
class MACEDONIA(PLACE):
    LIST = ['berovo','cesinovo','oblesevo','delcevo','karbinci','kocani','lozovo','makedonska','kamenica','pehcevo','probistip','stip','sveti',
    'nikole','vinica','zrnovci','kratovo','kriva','palanka','kumanovo','lipkovo','rankovce','staro','nagoricane','pelagonia','bitola',
    'demir','hisar','dolneni','krivogastani','krusevo','mogila','novaci','prilep','resen','polog','bogovinje','brvenica','gostivar','jegunovce',
    'mavrovo','rostusa','tearce','tetovo','vrapciste','zelino','skopje','aerodrom','aracinovo','butel','centar','cucer','sandevo',
    'gazi baba','gjorce','petrov','ilinden','karpos','kisela','petrovec','saraj','sopiste','studenicani','orizari','zelenikovo',
    'bogdanci','bosilovo','gevgelija','dojran','konce','novo selo','radovis','strumica','valandovo','vasilevo',
    'centar','debar','debarca','kicevo','makedonski','ohrid','plasnica','struga','vevcani',
    'vardar','caska','demir','kapija','gradsko','kavadarci','negotino','rosoman','veles']
    COUNTRY = "macedonia"     
    
    
class MALTA(PLACE):
    LIST = ['san gwann','victoria','tarxien','qormi','birkirkara','pauls','zabbar']
    COUNTRY = "malta"     
    
    
class MOLDOVA(PLACE):
    LIST = ['anenii','basarabeasca','briceni','cahul','cantemir','calarasi','causeni','cimislia','criuleni','donduseni','drochia',
    'dubasari','edinet','falesti','floresti','glodeni','hincesti','ialoveni','leova','nisporeni','ocnita','orhei','rezina',
    'riscani','singerei','soroca','straseni','soldanesti','stefan voda','taraclia','telenesti','ungheni','balti','bender','chisinau','gagauzia','transnistria']
    COUNTRY = "moldova"     
    
    
class MONACO(PLACE):
    LIST = ['monte carlo','spelugues','moulins','madone','rousse','saint roman','annonciade','chateau','perigord','larvotto','moulins',
    'larvotto','grace','michel','charlotte','condamine','colle','plati','pasteur',
    'revoires','hector','honore','labande','moneghetti','belgique','rainier','fontvieille']
    COUNTRY = "monaco"     
    
    
class MONTENEGRO(PLACE):
    LIST = ['andrijevica','bar','berane','bijelo','polje','budva','cetinje','danilovgrad','gusinje','herceg novi','kolasin','kotor',
    'mojkovac','niksic','petnjica','plav','pluzine','pljevlja','podgorica','rozaje','savnik','tivat','ulcinj','zabljak']
    COUNTRY = "montenegro"     
    
    
class NETHERLANDS(PLACE):
    LIST = ['drenthe','flevoland','fryslan','gelderland','groningen','limburg','brabant','holland','overijssel','utrecht','zeeland','friesland',
    'assen','lelystad','leeuwarden','arnhem','groningen','maastricht','hertogenbosch','haarlem','zwolle','hague','utrecht','middelburg','assen',
    'almere','leeuwarden','nijmegen','groningen','maastricht','eindhoven','amsterdam','enschede','rotterdam','utrecht','middelburg']
    COUNTRY = "netherlands"     
    
    
class NORWAY(PLACE):
    LIST = ['troms','finnmark','trondelag','trondelag','romsdal','fjordane','hordaland','rogaland','aust-agder','vest-agder','akershus','ostfold',
    'vestfold','telemark','buskerud','oppland','hedmark']
    COUNTRY = "norway"
         
    
class POLAND(PLACE):
    LIST = ['marszalek.','wielkopolskie','kuyavian','pomeranian','voivodeship','kujawsko','pomorskie','malopolskie','lodz',
    'odzkie','dolnoslaskie','lubelskie','lubusz','lubuskie','masovian','mazowieckie','opole','opolskie','podlaskie',
    'pomeranian','pomorskie','silesian','slaskie','subcarpathian','podkarpackie','swietokrzyskie','warmian','masurian',
    'warminsko','mazurskie)','pomeranian','zachodniopomorskie','poznan','bydgoszcz','torun','krakow','lodz','wroclaw',
    'lublin','gorzow','wielkopolski','andzielona','warsaw','opole','bialystok','gdansk','katowice','rzeszow','kielce','olsztyn','szczecin']
    COUNTRY = "poland"      
    
class PORTUGAL(PLACE):
    LIST = ['lisbon','leiria','santarem','setubal','beja','faro','evora','portalegre','castelo','branco','guarda',
    'coimbra','aveiro','viseu','braganca','vila real','porto','braga','viana','castelo']
    COUNTRY = "portugal"      
    
class ROMANIA(PLACE):
    LIST = ['bacau','botosani','iasi','neamt','suceava','vaslui','braila','buzau','constanta','galati','tulcea','vrancea','arges',
    'calarasi','dambovita','giurgiu','ialomita','prahova','teleorman','dolj','gorj','mehedinti','olt county','valcea','arad',
    'caras-severin','hunedoara','timis','bihor','bistrita','nasaud','cluj','maramures','satu mare','salaj','alba','brasov',
    'covasna','harghita','mures','ilfov','bucharest']
    COUNTRY = "romania"      
    
class RUSSIA(PLACE):
    LIST = ['adygea','bashkortostan','buryatia','altai','dagestan','ingushetia','kabardino','balkar','kalmykia','karachay','cherkess',
    'karelia','komi','mari','mordovia','sakha','yakutia','ossetia','alania','tatarstan','tuva','udmurt','khakassia','chechen',
    'chuvash','altai','krai','zabaykalsky','kamchatka','krasnodar','krasnoyarsk','perm','primorsky','stavropol','khabarovsk',
    'amur','arkhangelsk','astrakhan','belgorod','bryansk','vladimir','volgograd','vologda','voronezh','ivanovo','irkutsk',
    'kaliningrad','kaluga','kemerovo','kirov','kostroma','kurgan','kursk','leningrad','lipetsk','magadan','moscow','murmansk','nizhny',
    'novgorod','novgorod','novosibirsk','omsk','orenburg','oryol','penza','pskov','rostov','ryazan','samara','saratov','sakhalin',
    'sverdlovsk','smolensk','tambov','tver','tomsk','tula','tyumen','ulyanovsk','chelyabinsk','yaroslavl','moscow','petersburg',
    'nenets','okrug','khanty','mansi','yugra','chukotka','yamalo','nenets','crimea','sevastopol','maykop','ulan-ude','gorno','altaysk',
    'makhachkala','magas','nazran','nalchik','elista','cherkessk','petrozavodsk','syktyvkar','yoshkar','saransk',
    'yakutsk','vladikavkaz','kazan','kyzyl','izhevsk','abakan','grozny','cheboksary','barnaul','chita','petropavlovsk',
    'kamchatsky','krasnodar','krasnoyarsk','vladivostok','stavropol','khabarovsk','blagoveshchensk','arkhangelsk','astrakhan',
    'belgorod','bryansk','vladimir','volgograd','vologda','cherepovets','voronezh','ivanovo','irkutsk','kaliningrad','kaluga',
    'kemerovo','novokuznetsk','kirov','kostroma','kurgan','kursk','gatchina','lipetsk','magadan','balashikha','murmansk','nizhny',
    'novgorod','veliky','novosibirsk','omsk','orenburg','oryol','penza','pskov','rostov','ryazan','samara','saratov','yuzhno',
    'sakhalinsk','yekaterinburg','smolensk','tambov','tver','tomsk','tyumen','ulyanovsk','chelyabinsk','yaroslavl','birobidzhan',
    'naryan','khanty','mansiysk','surgut','anadyr','salekhard','urengoy','simferopol']
    COUNTRY = "russia"      
    
class MARINO(PLACE):
    LIST = ['acquaviva','borgo maggiore','chiesanuova','domagnano','faetano','fiorentino','montegiardino','serravalle']
    COUNTRY = "marino"      
    
class SERBIA(PLACE):
    LIST = ['kolubara','kolubarski','macva','macvanski','moravica','moravicki','pomoravlje','pomoravski','rasina','rasinski','raska',
    'raski','sumadija','sumadijski','zlatibor','zlatiborski','borski','branicevo','branicevski','jablanica','jablanicki','nisava',
    'nisavski','pcinja','pcinjski','pirot','pirotski','podunavlje','podunavski','toplica','toplicki','zajecar','zajecarski','banat',
    'srednjebanatski','backa','severnobacki','severnobanatski','juznobacki','juznobanatski','sremski','zapadnobacki','kosovo',
    'kosovski','pomoravlje','kosovsko','pomoravski','kosovska','mitrovica','kosovskomitrovicki','pecki','prizren','prizrenski']
    COUNTRY = "serbia"      
    
class SLOVAKIA(PLACE):
    LIST = ['kolubara','kolubarski','macva','macvanski','moravica','moravicki','pomoravlje','pomoravski','rasina',
    'rasinski','raska','raski','sumadija','sumadijski','zlatibor','zlatiborski','borski','branicevo','branicevski',
    'jablanica','jablanicki','nisava','nisavski','pcinja','pcinjski','pirot','pirotski','podunavlje','podunavski','toplica',
    'toplicki','zajecar','zajecarski','banat','srednjebanatski','backa','severnobacki','severnobanatski','juznobacki','juznobanatski',
    'sremski','zapadnobacki','kosovo','kosovski','pomoravlje','kosovsko','pomoravski','kosovska','mitrovica','kosovskomitrovicki',
    'pecki','prizren','prizrenski']
    COUNTRY = "slovakia"  
     

class SLOVENIA(PLACE):
	LIST = ['majsperk','preddvor','sveti','tomaz','makole','prevalje','salovci','maribor','sempeter','vrtojba','markovci','puconci','sencur','medvode','sentilj',
	'menges','radece','sentjernej','metlika','radenci','sentjur','mezica','radlje','dravi','sentrupert','miklavz','dravskem','polju','radovljica',
	'skocjan','miren','kostanjevica','ravne','koroskem','skofja','mirna','razkrizje','skofljica','mislinja','recica','savinji','smarje','jelsah',
	'mokronog','trebelno','rence','vogrsko','smarjeske','toplice','moravce','ribnica','smartno','moravske','toplice','ribnica','pohorju','smartno',
	'litiji','mozirje','rogaska','slatina','sostanj','murska','sobota','rogasovci','store','ajdovscina','rogatec','tabor','apace','naklo','tisina',
	'beltinci','nazarje','selnica','dravi','tolmin','benedikt','gorica','semic','trbovlje','bistrica','sotli','mesto','sevnica','trebnje','lukovica',
	'odranci','sezana','trnovska','bloke','oplotnica','slovenj','gradec','trzin','bohinj','ormoz','slovenska','bistrica','trzic','borovnica','osilnica',
	'slovenske','konjice','turnisce','bovec','pesnica','sodrazica','velenje','braslovce','piran','pirano','solcava','velika','polana','pivka',
	'sredisce','dravi','velike','lasce','brezovica','podcetrtek','starce','verzej','brezice','podlehnik','straza','videm','cankova','podvelka',
	'sveta','vipava','celje','poljcane','sveta','trojica','slovenskih','goricah','vitanje','cerklje','gorenjskem','polzela','sveta','andraz',
	'slovenskih','goricah','vodice','cerknica','postojna','sveti','jurij','vojnik','cerkno','prebold','sveti','jurij','slovenskih','goricah',
	'vransko','cerkvenjak','izola','isola','vrhnika','cirkulane','jesenice','vuzenica','crensovci','jezersko','zagorje','koroskem','jursinci',
	'zavrc','crnomelj','kamnik','zrece','destrnik','kanal','zalec','divaca','kidricevo','zelezniki','dobje','kobarid','zetale','dobrepolje',
	'kobilje','dobrna','kocevje','zirovnica','dobrova','polhov','gradec','komen','zuzemberk','dobrovnik','dobronak','komenda','ljubljani','koper',
	'capodistria','dolenjske','toplice','kosanjevica','domzale','kostel','dornava','kozje','dravograd','kranj','duplek','kranjska','gorenja',
	'poljane','krizevci','gorisnica','krsko','gorje','kungota','gornja','radgona','kuzma','gornji','lasko','gornji','petrovci','lenart','lendava',
	'lendva','grosuplje','litija','hajdina','ljubljana','slivnica','ljubno','hodos','hodos','ljutomer','horjul','dragomer','hrastnik','logatec',
	'hrpelje','kozina','loska','dolina','idrija','loski','potok','lovrenc','pohorju','ilirska','bistrica','ivancna','gorica']
	COUNTRY = 'slovenia'    
    
class SPAIN(PLACE):
	LIST = ['andalucia','alava','palmas','castellon','salamanca','zaragoza','aragon','pontevedra','albacete','asturias,','principado','ciudad','rioja',
	'santa','tenerife','ceuta','melilla','canarias','alicante','lleida','cordoba','segovia','cantabria','zamora','almeria','castilla','mancha',
	'cuenca','sevilla','castilla','asturias','madrid','girona','soria','catalunya','avila','malaga','extremadura','granada','tarragona','galicia',
	'badajoz','murcia','guadalajara','teruel','illes','balears','barcelona','navarra','nafarroa','rioja','guipuzcoa','gipuzkoa','toledo','madrid,',
	'comunidad','burgos','ourense','huelva','valencia','valencia','murcia,','region','caceres','palencia','navarra,','comunidad','foral','nafarroako',
	'komunitatea','huesca','valladolid','vasco','euskal','herria','cadiz','balears','vizcayaa','bizkaia','valenciana,','comunidad','valenciana,',
	'comunitat','cantabria','coruna']
	COUNTRY = 'spain'     
    
class SWEDEN(PLACE):
	LIST = ['vasternorrlands','gavleborgs','kronobergs','sodermanlands','vastmanlands','hallands','norrbottens','blekinge','uppsala','vastra',
	'gotalands','jamtlands','skane','dalarnas','varmlands','orebro','jonkopings','stockholms','gotlands','ostergotlands','kalmar','vasterbottens']
	COUNTRY = 'sweden'
	
class SWITZERLAND(PLACE):
	LIST = ['solothurn','schwyz','aargau','thurgau','appenzell','innerrhoden','ticino','appenzell','ausserrhoden','basel','landschaft',
	'valais','basel','stadt','fribourg','zurich','geneve','glarus','graubunden','luzern','neuchatel','nidwalden',
	'obwalden','sankt','gallen','schaffhausen']
	COUNTRY = 'switzerland'
	
class TURKEY(PLACE):
	LIST = ['giresun','sivas','bilecik','kocaeli','gumushane','sanliurfa','bingol','konya','hakkari','sirnak','bitlis',
	'kutahya','adana','hatay','malatya','tekirdag','adiyaman','igdir','burdur','manisa','tokat','afyonkarahisar',
	'isparta','bursa','mardin','trabzon','istanbul','canakkale','mersin','tunceli','aksaray','izmir','cankiri',
	'kilis','mugla','amasya','kahramanmaras','corum','ankara','karabuk','denizli','nevsehir','sinop','yalova',
	'antalya','karaman','diyarbakir','nigde','yozgat','ardahan','duzce','zonguldak','artvin','kastamonu','edirne',
	'osmaniye','aydin','kayseri','elazig','balikesir','kirikkale','erzincan','sakarya','bartin','kirklareli','erzurum',
	'samsun','batman','kirsehir','eskisehir','siirt','bayburt','gaziantep']
	COUNTRY = 'turkey'
	
class UKRAINE(PLACE):
	LIST = ['zhytomyrs','ivano','frankivs','respublika','kharkivs','kyivs','khersons',
	'sevastopol','khmel','kirovohrads','kyivs','luhans','mykolaivs',
	'oblast','poltavs','rivnens','cherkas','ternopil','chernihivs',
	'vinnyts','chernivets','volyns','dnipropetrovs','zakarpats','donets',
	'zaporiz']
	COUNTRY = 'ukraine'
	
class UNITED_KINGDOM(PLACE):
	LIST = ['camden','gateshead','carmarthenshire','gaerfyrddin','croydon','kirklees','ceredigion','ceredigion','ealing','knowsley','conwy',
	'enfield','leeds','denbighshire','ddinbych','greenwich','liverpool','flintshire','fflint','hackney','manchester','gwynedd',
	'hammersmith','fulham','newcastle','anglesey','haringey','north','tyneside','merthyr','tydfil','merthyr','tudful',
	'harrow','oldham','monmouthshire','fynwy','havering','rochdale','neath','talbot','castell','talbot','hillingdon',
	'rotherham','newport','casnewydd','hounslow','helens','pembrokeshire','benfro','islington','salford','powys','kensington','chelsea',
	'sandwell','rhondda,','cynon,','rhondda,','cynon,taf','kingston','thames','sefton','swansea','abertawe','lambeth',
	'sheffield','torfaen','lewisham','solihull','glamorgan,','morgannwg','merton','south','tyneside','wrexham','wrecsam',
	'newham','stockport','redbridge','sunderland','richmond','thames','tameside','southwark','trafford','sutton','wakefield',
	'tower','hamlets','walsall','waltham','forest','wigan','wandsworth','wirral','westminster','wolverhampton','barnsley',
	'london,','birmingham','aberdeen','bolton','aberdeenshire','bradford','angus','wales','argyll','calderdale','clackmannanshire',
	'coventry','dumfries','galloway','doncaster','dundee','dudley','ayrshire','northern','ireland','dunbartonshire','derry',
	'luton','england','wales','lothian','craigavon','medway','great','britain','renfrewshire','dungannon','south','tyrone',
	'middlesbrough','united','kingdom','edinburgh,','fermanagh','milton','keynes','leicester','buckinghamshire','eilean','larne',
	'north','lincolnshire','cambridgeshire','falkirk','limavady','north','lincolnshire','cardiff','caerdydd','cumbria','lisburn',
	'north','somerset','derbyshire','glasgow','magherafelt','northumberland','devon','highland','moyle','nottingham','dorset',
	'inverclyde','newry','mourne','peterborough','sussex','midlothian','newtownabbey','plymouth','essex','moray','north',
	'poole','gloucestershire','north','ayrshire','omagh','portsmouth','hampshire','north','lanarkshire','strabane','reading',
	'hertfordshire','orkney','north','somerset','redcar','cleveland','perth','kinross','blackburn','darwen',
	'rutland','lancashire','renfrewshire','bedford','shropshire','leicestershire','scottish','borders,','blackpool','slough','lincolnshire',
	'shetland','bournemouth','south','gloucestershire','norfolk','south','ayrshire','bracknell','forest','southampton',
	'north','yorkshire','south','lanarkshire','brighton','southend','northamptonshire','stirling','bristol,','stockton','nottinghamshire',
	'dunbartonshire','central','bedfordshire','stoke','trent','oxfordshire','lothian','cheshire','swindon','somerset','antrim',
	'cheshire','chester','telford','wrekin','staffordshire','cornwall','thurrock','suffolk','armagh','darlington','torbay',
	'surrey','ballymena','derby','warrington','warwickshire','ballymoney','durham,','county','berkshire','sussex','banbridge',
	'riding','yorkshire','windsor','maidenhead','worcestershire','belfast','halton','wokingham','barking','dagenham','carrickfergus',
	'hartlepool','barnet','castlereagh','herefordshire','blaenau','gwent','england','bexley','coleraine','wight','bridgend',
	'scotland','brent','cookstown','kingston','caerphilly','caerffili','bromley','england','scotland',' eng ',' eng. ','scotia']
	COUNTRY = 'united kingdom'
	
	
#=============================== Get Country and Year ==================================


EUROPE = list()
EUROPE.append(UKRAINE())
EUROPE.append(TURKEY())
EUROPE.append(SWITZERLAND())
EUROPE.append(SWEDEN())
EUROPE.append(SLOVENIA())
EUROPE.append(SLOVAKIA())
EUROPE.append(SERBIA())
EUROPE.append(MARINO())
EUROPE.append(RUSSIA())
EUROPE.append(ROMANIA())
EUROPE.append(PORTUGAL())
EUROPE.append(POLAND())
EUROPE.append(NORWAY())
EUROPE.append(NETHERLANDS())
EUROPE.append(MONTENEGRO())
EUROPE.append(MONACO())
EUROPE.append(MOLDOVA())
EUROPE.append(MALTA())
EUROPE.append(MACEDONIA())
EUROPE.append(LUXEMBOURG())
EUROPE.append(LITHUANIA())
EUROPE.append(LIECHTENSTEIN())
EUROPE.append(LATVIA())
EUROPE.append(KOSOVO())
EUROPE.append(KAZAKHSTAN())
EUROPE.append(HUNGARY())
EUROPE.append(GREECE())
EUROPE.append(GEORGIA())
EUROPE.append(FRANCE())
EUROPE.append(FINLAND())
EUROPE.append(DENMARK())
EUROPE.append(CZECH())
EUROPE.append(CYPRUS())
EUROPE.append(CROATIA())
EUROPE.append(BULGARIA())
EUROPE.append(BOSNIA())
EUROPE.append(ALBANIA())
EUROPE.append(ANDORRA())
EUROPE.append(ARMENIA())
EUROPE.append(AUSTRIA())
EUROPE.append(AZERBAIJAN())
EUROPE.append(GERMANY())
EUROPE.append(ITALY())
EUROPE.append(SPAIN())
EUROPE.append(UNITED_KINGDOM())
EUROPE.append(BELARUS())
EUROPE.append(BELGIUM())


AMERICA = list()
AMERICA.append(BARBUDA())
AMERICA.append(BAHAMAS())
AMERICA.append(BARBADOS())
AMERICA.append(BARBADOS())
AMERICA.append(BELIZE())
AMERICA.append(CANADA())
AMERICA.append(COSTA_RICA())
AMERICA.append(CUBA())
AMERICA.append(DOMINICA())
AMERICA.append(DOMINICAN())
AMERICA.append(SALVADOR())
AMERICA.append(GUATEMALA())
AMERICA.append(HAITI())
AMERICA.append(HONDURAS())
AMERICA.append(JAMAICA())
AMERICA.append(NICARAGUA())
AMERICA.append(PANAMA())
AMERICA.append(NEVIS())
AMERICA.append(LUCIA())
AMERICA.append(GRENADINES())
AMERICA.append(TRINIDAD())
AMERICA.append(ARGENTINA())
AMERICA.append(MEXICO())
AMERICA.append(USA())
AMERICA.append(BOLIVIA())
AMERICA.append(BRAZIL())
AMERICA.append(CHILE())
AMERICA.append(COLOMBIA())
AMERICA.append(ECUADOR())
AMERICA.append(GUYANA())
AMERICA.append(PARAGUAY())
AMERICA.append(PERU())
AMERICA.append(SURINAME())
AMERICA.append(URUGUAY())
AMERICA.append(VENEZUELA())





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


#def extract_country(string):
#    global PLACES
#
#    string = " " + unidecode(string.lower()) + " "
#
#    for country in COUNTRIES.AMERICA:
#        if country in string:
#            return country
#            
#    for country in COUNTRIES.EUROPE:
#        if country in string:
#            return country
#            
#    for country in COUNTRIES.ASIA:
#        if country in string:
#            return country
#            
#    for country in COUNTRIES.AFRICA:
#        if country in string:
#            return country
#            
#    for country in COUNTRIES.OCEANIA:
#        if country in string:
#            return country
#            
#            
#    # others
#    
#    
#    for PLACE in AMERICA:
#        for LOCATION in PLACE.LIST:
#            if LOCATION in string:
#                return PLACE.COUNTRY
#                
#                
#    for PLACE in EUROPE:
#        for LOCATION in PLACE.LIST:
#            if LOCATION in string:
#                return PLACE.COUNTRY
#    
#            
#    return None




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
        


    
# This function cleans a country string by trimming the string and grabbing just alpha characters
#def just_alpha(string):
#    string = string.strip().lower()
#    string = unidecode(string)
#    match = re.search(r'(?:[^\W\d_]| )+', string)
#    if match is not None:
#        return match.group(0)
#    else:
#        #print("PARSE YEAR ERROR date: " + string.encode("utf-8"))
#        return None




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
        #print("PARSE YEAR ERROR date: " + string.encode("utf-8"))
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
                        #array = value.split(',')
                        #noisy_country = array[len(array) - 1]
                        #noisy_country = just_alpha(noisy_country)
                        #if noisy_country is not None:
                            #country = noisy_country
                            #break
                        
        elif info_type == INFO_TYPE.FORMAL:
            if INFO_TYPE.FORMAL in place:
                formal = place.get(INFO_TYPE.FORMAL)
                country = extract_country(formal)
                #print("FORMAL PLACE " + str(formal.encode("utf-8")))
        
        elif info_type == INFO_TYPE.ORIGINAL:
            if INFO_TYPE.ORIGINAL in place:
                original = place.get(INFO_TYPE.ORIGINAL)
                country = extract_country(original)
                #words = original.split(',')
                #if len(words) > 2:
                    #noisy_country = words[len(words) - 1]
                    #noisy_country = just_alpha(noisy_country)
                    #if noisy_country is not None and len(noisy_country) > 4 or noisy_country == "peru":
                        #country = noisy_country
                    #elif noisy_country is not None:
                        #print("ORIGINAL NOISY COUNTRY " + noisy_country)
          

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
                            #print("NORMALIZED DATE " + str(year))
                            break
                        #elif noisy_year is not None:
                            #print("NORMALIZED NOISY YEAR " + noisy_year + " ALL DATE " + str(normal.encode("utf-8")))
                    
        elif info_type == INFO_TYPE.FORMAL:
            if INFO_TYPE.FORMAL in date:
                formal = date.get(INFO_TYPE.FORMAL)
                noisy_year = just_numbers(formal)
                if noisy_year is not None and len(noisy_year) > 2 and int(noisy_year) > 100:
                    year = int(noisy_year)
                    #print("FORMAL DATE " + str(year))
                #elif noisy_year is not None:
                    #print("FORMAL NOISY YEAR " + noisy_year + " ALL DATE " + str(formal.encode("utf-8")))
                 
        elif info_type == INFO_TYPE.ORIGINAL:
            if INFO_TYPE.ORIGINAL in date:
                original = date.get(INFO_TYPE.ORIGINAL)
                words = original.split(" ")
                if len(words) > 2:
                    noisy_year = words[len(words) - 1]
                    noisy_year = just_numbers(noisy_year)
                    if noisy_year is not None and len(noisy_year) > 2 and int(noisy_year) > 100:
                        year = int(noisy_year)
                        #print("ORIGINAL DATE " + str(year))
                    #elif noisy_year is not None:
                        #print("ORIGINAL NOISY YEAR " + noisy_year + " ALL DATE " + str(original.encode("utf-8")))
    
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
                    birth_countries.extend(countries) # using residence fact to estimate birth place, but not birth year
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
    #if len(line) > 200000:
    #    print("data: " + str(line.encode("utf-8")) + "\n\n\n")

    
    # we get estimates of birth the country-year pair, and the death country-year pair
    # from the principal person or from related people
    person = get_info(person_id, persons_json, relationships_json)
    
    
    # we round years
    if person.birth_year is not None:
        person.birth_year = round(person.birth_year, BUCKET_SIZE)
    if person.death_year is not None:
        person.death_year = round(person.death_year, BUCKET_SIZE)

    
    # we make te output list that will be used by spark
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
inputFolder = home + '/Documents/classes/Bigdata2/data/' #'/fsl_groups/fslg_familytree/compute/TreeData/'
outputFolder = home + '/Documents/classes/Bigdata2/FamilyTree/compute/' #'/compute/family_search/'


lines = sc.textFile(inputFolder + '*.json.gz')
persons = lines.flatMap(lambda line: get_person_info(line))
persons = persons.map(lambda x: (x, 1))
persons = persons.reduceByKey(lambda a, b: a+b)
persons = persons.sortByKey(True)
persons.saveAsTextFile(outputFolder)

   
    
    
    
 



