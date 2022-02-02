%let pgm=utl-merging-two-tables-without-any-common-column-data-in-r-python-and-sas;

Merging two tables without any common column data in r python and sas

github
https://tinyurl.com/y3ut37nd
https://github.com/rogerjdeangelis/utl-merging-two-tables-without-any-common-column-data-in-r-python-and-sas

What is nice that R, SAS and Python use basically the same SQL code

   Six Solutions
     1. SAS merge
        merge sd1.hav1st hav2nd(rename=(x=a y=b));
        xx=cats(a,x);
        yy=cats(b,y);
     2, SAS sql
        select
            cats(t2.x,t1.x) as xx
           ,cats(t2.y,t1.y) as yy
        from
            ( select monotonic() as rownum, x, y from hav1st ) as t1
           ,( select monotonic() as rownum, x, y from hav2nd ) as t2
        where
            t1.rownum = t2.rownum
     3. r bind
        r_bnd<-as.data.frame(cbind(paste0(hav2nd$X,hav1st$X),paste0(hav2nd$Y,hav1st$Y)));
     4. Python bind (python has too many data structures/objects/data types - worse than R)
        I am not including it (seems strange- not easily understood?) use sql instead?
     5. r sql (basically the same as SAS SQL)
     6. python sql (basically the same as SAS SQL)

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.hav1st;
  informat x y $1.;
 input x y ;
cards4;
1 5
2 6
3 7
4 8
;;;;
run;quit;

data sd1.hav2nd;
  informat x y $1.;
input x y;
cards4;
A E
B F
C G
D H
;;;;
run;quit;

/*
TWO TABLES

   SD1.HAV2ND      SD1.HAV1ST
  =============   =============
  Obs    X    Y   Obs    X    Y

   1     A    E    1     1    5
   2     B    F    2     2    6
   3     C    G    3     3    7
   4     D    H    4     4    8
             _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
Obs    XX    YY

 1     A1    E5
 2     B2    F6
 3     C3    G7
 4     D4    H8

/*___  ____   ___   ____ _____ ____ ____
|  _ \|  _ \ / _ \ / ___| ____/ ___/ ___|
| |_) | |_) | | | | |   |  _| \___ \___ \
|  __/|  _ <| |_| | |___| |___ ___) |__) |
|_|   |_| \_\\___/ \____|_____|____/____/
                           _    ___
 ___  __ _ ___   ___  __ _| |  ( _ )    _ __ ___   ___ _ __ __ _  ___
/ __|/ _` / __| / __|/ _` | |  / _ \/\ | `_ ` _ \ / _ \ `__/ _` |/ _ \
\__ \ (_| \__ \ \__ \ (_| | | | (_>  < | | | | | |  __/ | | (_| |  __/
|___/\__,_|___/ |___/\__, |_|  \___/\/ |_| |_| |_|\___|_|  \__, |\___|
                        |_|                                |___/
*/

proc sql;
  create
      table want_sql as
  select
      cats(t2.x,t1.x) as xx
     ,cats(t2.y,t1.y) as yy
  from
      ( select monotonic() as rownum, x, y from hav1st ) as t1
     ,( select monotonic() as rownum, x, y from hav2nd ) as t2
  where
  t1.rownum = t2.rownum
;quit;
data sas_mrg;
  merge sd1.hav1st hav2nd(rename=(x=a y=b));
  xx=cats(a,x);
  yy=cats(b,y);
  drop x--b;
run;quit;

/*                _    ___          _     _           _
 _ __   ___  __ _| |  ( _ )     ___| |__ (_)_ __   __| |
| `__| / __|/ _` | |  / _ \/\  / __| `_ \| | `_ \ / _` |
| |    \__ \ (_| | | | (_>  < | (__| |_) | | | | | (_| |
|_|    |___/\__, |_|  \___/\/  \___|_.__/|_|_| |_|\__,_|
               |_|
*/

%utl_submit_r64('
   library(haven);
   library(sqldf);
   library(sqldf);
   library(SASxport);
   hav1st<-read_sas("d:/sd1/hav1st.sas7bdat");
   hav2nd<-read_sas("d:/sd1/hav2nd.sas7bdat");
   r_sql<-sqldf("
      select
          t2.x||t1.x as xx
         ,t2.y||t1.y as yy
      from
          ( select rowid  as row, x, y from hav1st ) as t1
         ,( select rowid  as row, x, y from hav2nd ) as t2
      where
          t1.row = t2.row
      ");
   r_bnd<-as.data.frame(cbind(paste0(hav2nd$X,hav1st$X),paste0(hav2nd$Y,hav1st$Y)));
   r_bnd;
   write.xport(r_sql,r_bnd,file="d:/xpt/r_mrg.xpt");
   ')

   r_bnd$col1<-paste0(r_bnd[,3],r_bnd[,1]);
   r_bnd$col2<-paste0(r_bnd[,4],r_bnd[,2]);
libname xpt xport "d:/xpt/r_mrg.xpt";

proc contents data=xpt._all_;
run;quit;

/*
TWO EXPORT FILE IN ONE XPT FILE

The CONTENTS Procedure

           Directory

Libref         XPT
Engine         XPORT
Physical Name  d:\xpt\r_mrg.xpt

          Member  Obs, Entries
#  Name   Type     or Indexes   Vars

1  R_SQL  DATA         .         2
2  R_BND  DATA         .         2

proc print data=xpt.r_sql;
proc print data=xpt.r_bnd;
run;quit;

Obs    XX    YY

  1    A1    E5
  2    B2    F6
  3    C3    G7
  4    D4    H8
*/

/*           _   _                             _
 _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
| .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
|_|    |___/                                |_|
*/

%utlfkil(d:/xpt/havchk.xpt);

%utl_submit_py64_39("
from os import path;
import pandas as pd;
import xport;
import xport.v56;
import pyreadstat;
import numpy as np;
from pandasql import sqldf;
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
hav1st, meta = pyreadstat.read_sas7bdat('d:/sd1/hav1st.sas7bdat');
hav2nd, meta = pyreadstat.read_sas7bdat('d:/sd1/hav2nd.sas7bdat');
havchk=pdsql('''
      select
          t2.x||t1.x as xx
         ,t2.y||t1.y as yy
      from
          ( select rowid  as row, x, y from hav1st ) as t1
         ,( select rowid  as row, x, y from hav2nd ) as t2
      where
          t1.row = t2.row
''');
print(havchk);
havchk.info();
ds = xport.Dataset(havchk, name='havchk');
with open('d:/xpt/havchk.xpt', 'wb') as f: xport.v56.dump(ds, f);
");

libname xpt xport "d:/xpt/havchk.xpt";

proc contents data=xpt._all_;
run;quit;

proc print data=xpt.havchk;
run;quit;
