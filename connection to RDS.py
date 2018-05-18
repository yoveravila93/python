#RDS

import pandas
from sqlalchemy import create_engine
import psycopg2 as pg

#rds nueva
#db_origin
engine = create_engine("postgresql://root:Madara19@db-devso.ckfoflpkrdae.us-east-2.rds.amazonaws.com:5432/db_postdevso", echo = True, use_batch_mode = True, isolation_level="READ UNCOMMITTED", server_side_cursors=True)
connect_db_postdevso = engine.connect()
connect_db_postdevso = connect_db_postdevso.execution_options(isolation_level="READ COMMITTED")

#new db
engine = create_engine("postgresql://root:Madara19@db-connet.ckfoflpkrdae.us-east-2.rds.amazonaws.com:5432/db_connet", echo = True, use_batch_mode = True, isolation_level="READ UNCOMMITTED", server_side_cursors=True)
connect_db_connect = engine.connect()
connect_db_connect = connect_db_connect.execution_options(isolation_level="READ COMMITTED")

chunksize = 1000
data = pandas.read_sql("""select * from "tbl_indicator" order by 1""", connect_db_postdevso)

data.to_sql('tbl_country', connect_db_connect, schema='public', if_exists='append', index=False, chunksize=chunksize)
