# -*- coding: utf-8 -*-
"""
Created on Fri Oct  6 11:35:11 2023

@author: Youri.Baeyens
"""

import sys
import config
import pandas as pd
import hashlib

sys.path.insert(1, config.mygeodb)

from geodatabase import Geodatabase as gdb

shapefiles2load=[
'Bpn_CaBu',
'Bpn_ReBu',
    ]

# %% Create database
db=gdb('../output/varyingCellSizeGrid_testData.sqlite',srid=31370)

# parameters
x_min=3924*1000
y_min=3081*1000

# %% Chargement
path=config.agdp
for shapefile in shapefiles2load:
    print(f'Load {shapefile}.')
    db.loadShp(f'{path}/{shapefile}', shapefile,encoding='utf-8')
    db.createSpatialIndex(f'{shapefile}')

# %% Zone étudiée = une zone à cheval sur le Brabant wallon, le Brabant flamand et Bruxelles
def cellWkt(x,y,l):
    return "POLYGON(("+\
            str(x+0*l)+" "+str(y+0*l)+","+\
            str(x+1*l)+" "+str(y+0*l)+","+\
            str(x+1*l)+" "+str(y+1*l)+","+\
            str(x+0*l)+" "+str(y+1*l)+","+\
            str(x+0*l)+" "+str(y+0*l)+"))"
            
studyZone=cellWkt(x_min,y_min,(13+10)*1000)

# %% Extraire le centroide des batiments dans la zone étudiée
db.dropTable('T01_points_in_studyZone')
db.database.execute(f'''

create table T01_points_in_studyZone as                 
select st_pointOnSurface(geometry) as geometry
from bpn_rebu
where rowid in (
  select rowid 
  from SpatialIndex 
  where f_table_name='bpn_rebu' 
    and search_frame=transform(GeomFromText('{studyZone}',3035),31370)
    )
UNION
select st_pointOnSurface(geometry) as geometry
from bpn_cabu
where rowid in (
  select rowid 
  from SpatialIndex 
  where f_table_name='bpn_cabu' 
    and search_frame=transform(GeomFromText('{studyZone}',3035),31370)
    )

''')

db.recoverGeometry('T01_points_in_studyZone')
db.createSpatialIndex('T01_points_in_studyZone')

# %% Chargement des secteurs statistiques dans la db

db.attach(config.secteurs, 'secteurs')
db.dropTable('T01_statistical_sectors')
db.database.execute('''
create table T01_statistical_sectors as
select cd_sector, CastToXY(geometry) as geometry
from secteurs.sh_statbel_statistical_sectors_31370_20230101
''')
db.recoverGeometry('T01_statistical_sectors')
db.createSpatialIndex('T01_statistical_sectors')

# %% Mettre les points dans les polygones 

db.dropTable('T02_points_in_sectors')
db.pointInPolygon('main.T01_points_in_studyZone'.lower(), 
                  'T01_statistical_sectors'.lower(), 
                  'T02_points_in_sectors')

db.dropTable('T03_points_in_studyZone_with_sectors')
db.database.execute('''
create table T03_points_in_studyZone_with_sectors as
select X(transform(A.geometry,3035)) as X, Y(transform(A.geometry,3035)) as Y, c.cd_sector as cd_sector, a.geometry as geometry
from  T01_points_in_studyZone A, T02_points_in_sectors B, T01_statistical_sectors C
where A.rowid=b.rowid_point and b.rowid_polygon=c.rowid 
''')


# %% Chargement des données de population par secteur

T03_population_by_sectors=pd.read_csv('../input/OPENDATA_SECTOREN_2022.txt',sep='|')
T03_population_by_sectors[['CD_SECTOR','TOTAL']].to_sql('T03_population_by_sectors',db.database,if_exists='replace')

# %% Plan pour le tirage d'échantillon avec remise

T05_plan=pd.read_sql(
'''

with T04_count_of_points_by_sector as (
        
    select cd_sector as cd_sector, count(*) ms_countOf_points_in_sector
    from T03_points_in_studyZone_with_sectors 
    group by 1
)

select e.cd_sector, 
       e.ms_countOf_points_in_sector, 
       f.total as ms_population_in_sector
from  T04_count_of_points_by_sector e,
      T03_population_by_sectors f
where e.cd_sector=f.cd_sector
                   
''', db.database)

# %% Tirage de l'échantillon avec remise

db.dropTable('T06_sample')
for s in T05_plan.itertuples():
    T06_sample=pd.read_sql(f"""
                           select * 
                           from T03_points_in_studyZone_with_sectors 
                           where cd_sector='{s.cd_sector}'""",
                           db.database).sample(s.ms_population_in_sector,replace=True)
    T06_sample.to_sql('T06_sample',db.database,if_exists='append')

# %% Exportation des résultats vers un fichier csv
    
def md5sum(t):
    return hashlib.md5(str(t).encode()).hexdigest()[0:16]

db.database.create_function("md5", 1, md5sum)

pd.read_sql(f'''
  select md5(rowid) as id_demo, 
         md5(cast(rank() over (partition by A.geometry order by rowid)/6 as string)||"-"||X||"-"||Y) as id_hh, 
         x, 
         y, 
         cd_sector
  from T06_sample A
  where {x_min}<x and x<{x_min+13*1000}
    and {y_min}<y and y<{y_min+13*1000}
  order by 2, 1
''',db.database).to_csv('../output/synthetic_population.csv',sep='|',index=False)

# %% Fermeture de la connexion vers la db

db.close()    