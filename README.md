Vigilance Modeller
==================
Vigilance Modeller is a predictive burglary modeller based on the work by Dr. Shane Johnson of the UCL, Jill Dando Institute of Crime Science, primarily *Prospective Crime Mapping in Operational Context: Final Report* - [On-line Report 19/07], London: Home Office (Johnson, S. D., Birks, D., McLaughlin, L., Bowers, K. J. & Pease, K. (2007)).

/vigmod
-------
Django application for accepting CSV uploads and processing the data through the Vigilance Modeller database.
Requires:
* [Django] - awesome Open Source Python web application framework
* [OGR] - fantastic Open Source vector file library

**Note**: Tested only with [PostgreSQL] 9.0 back-end using [Psycopg] 2, [Python] 2.7

/sql
----
Functions for **vigmod_data** database. These segregate burglary data based on a grid and dates and use that to generate the predicted likelihood of further offences in the cells of that grid.


Licence
-------
MIT

[On-line Report 19/07]:http://www.academia.edu/973382/Prospective_Crime_Mapping_in_Operational_Context_Final_Report
[Django]:https://www.djangoproject.com/
[OGR]:http://www.gdal.org/ogr/
[PostgreSQL]:http://www.postgresql.org/
[Psycopg]:http://initd.org/psycopg/
[Python]:https://www.python.org/