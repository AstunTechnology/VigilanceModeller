import csv
import os
import string
import psycopg2
import zipfile
import logging
import logging.handlers

from datetime import datetime, timedelta


import django.template
from django.core.mail import EmailMultiAlternatives
from django.core.management.base import BaseCommand, CommandError

from vigmod.fileupload.conf import settings
from vigmod.fileupload.models import Filestore

class Command(BaseCommand):
    help = 'Manages the queue of data, if database is ready, imports next file'
    


    def handle(self, *args, **options): 

        # Enable logging
        LOG_FILENAME = 'processqueue.log'

        # Set up a specific logger with our desired output level
        self.logger = logging.getLogger('Vigilance Modeller Queue Logger')
        self.logger.setLevel(logging.DEBUG)

        # Add the log message handler to the logger
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=1048576, backupCount=5)
        handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        
        self.logger.addHandler(handler)

       
        # Check to see if jobs are marked as running, if they are then exit
        if len(Filestore.objects.filter(processed=-2)):
            print 'Data already being processed.\n'
            return

        # Check for processed data older than allowed time

        self.purgedata(
            Filestore.objects.filter(
                processed=1,
                processeddate__lt=datetime.now() - timedelta(hours=settings.PURGEHOURS)
            ).order_by(
                'processeddate'
            )
        )   

       

        # If data is ready then start processing
        try:
            self.processdata(
                Filestore.objects.filter(
                    processed=-1
                ).order_by(
                    'verificationdate'
                )[0]    
            )
            return            
        except IndexError:
            print 'Nothing waiting for processing.\n'
        
        # Get the next unimported job in the queue 
        # Checks for earliest verified date and not already (being) processed
        # TODO?: import all available data
        try:
            self.importdata(
                Filestore.objects.filter(
                    processed=0,
                    emailverified=1
                ).order_by(
                    'verificationdate'
                )[0]    
            )            
        except IndexError:
            print 'No files remain to be processed.'

    def purgedata(self, jobs):
        if(len(jobs)==0):            
            print 'No jobs need to be purged.\n'
            return
        #print jobs

    
    def processdata(self, job):
        '''processdata: Runs data through prospective mapping process'''

        
        connection_string = "dbname=%s user=%s password=%s" % \
            ( 
                settings.postgresql['dbname'], \
                settings.postgresql['user'], \
                settings.postgresql['password']
            )
           
        # Open connection to PostgreSQL
        
        self.logger.debug('Open database connection: ' + connection_string)
        dbconn = conn = psycopg2.connect(connection_string)
        cur = dbconn.cursor()        
        self.logger.debug('Database connection open.')
        
        msg = '%s - Processing started on job with id %s' % \
             (datetime.now(),job.guid)
        
        self.logger.info(msg)
        print(msg)
        
        # Run main processing function
        sql = "select create_risk_matrix( \
'%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s')"% \
        (
            job.guid,
            'dataset_'+job.guid+'_import',
            settings.matrix['cell_size'],
            '', #bounding box
            '', #first date
            '', #last date
            settings.matrix['spatial_bandwidth'], 
            settings.matrix['temporal_bandwidth']
        )          
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)
        dbconn.commit()

        sql =  "select matrix_table from meta_data where dataset_name = '%s'" % \
            job.guid
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)

        # Get spatial output table name
        matrix_name = cur.fetchone()[0]

        # Close database connection
        self.logger.debug('Closing database connection.')
        cur.close()
        dbconn.close()      
        self.logger.debug('Database connection closed.')

        # Run ogr2ogr to output spatial files
        # ...should perhaps use Python ogr library

        risks_statement = ' \
select geom, week_commencing, risk_intensity::real as risk from ' + matrix_name
        
        # ...MapInfo
        msg = '\tExporting to MapInfo...'        
        self.logger.info(msg)
        print(msg)

        # Extract data to MapInfo files
        ogr = '%s -a_srs "EPSG:27700" -f "MapInfo File" %s PG:"%s" -sql "%s"' %\
        (
            settings.paths['ogr']+'ogr2ogr.exe', 
            settings.FILESAVEPATH+job.guid+'.tab',
            connection_string,
            risks_statement
        )   
        self.logger.debug(ogr)
        os.system(ogr)
        
        # Zip MapInfo files
        self.logger.debug('Zipping MapInfo files:')
        mapinfo_exts = ['tab','id','map','dat']
        mapinfo_zip = zipfile.ZipFile(
            settings.FILESAVEPATH+job.guid+'_MapInfo.zip',
            'w'
        )
        for ext in mapinfo_exts:
            filename = job.guid+'.'+ext
            mapinfo_zip.write(settings.FILESAVEPATH+filename,filename)            
            self.logger.debug('\t'+filename)
        mapinfo_zip.close()
        self.logger.debug('MapInfo zipped.')

        
        # ...Shapefile
        msg =  '\tExporting to Shapefile...'      
        self.logger.info(msg)
        print(msg)

        # Remove old Shapefile files
        self.logger.debug('Removing old Shapefiles:')
        shapefile_exts = ['shp','shx','dbf','prj']
        for ext in shapefile_exts:
            try:
                os.remove(settings.FILESAVEPATH+job.guid+'.'+ext)        
                self.logger.debug('\t'+settings.FILESAVEPATH+job.guid+'.'+ext)
            except:
                pass
        self.logger.debug('Old Shapefiles removed or none found.')
                

        # Extract data to Shapefiles        
        ogr =  '%s -a_srs "EPSG:27700" -f "ESRI Shapefile" %s PG:"%s" -sql "%s"' %\
        (
            settings.paths['ogr']+'ogr2ogr.exe', 
            settings.FILESAVEPATH+job.guid+'.shp',
            connection_string,
            risks_statement
        )            
        self.logger.debug(ogr)
        os.system(ogr)
        
        # Zip Shapefile files
        self.logger.debug('Zipping Shapefiles:')
        shapefile_zip = zipfile.ZipFile(
            settings.FILESAVEPATH+job.guid+'_Shapefile.zip',
            'w'
        )
        for ext in shapefile_exts:
            filename = job.guid+'.'+ext
            shapefile_zip.write(settings.FILESAVEPATH+filename,filename)           
            self.logger.debug('\t'+filename)
        shapefile_zip.close()
        self.logger.debug('Shapefiles zipped.')
        
        
        msg =  '%s - Data processed for job %s' % (datetime.now(),job.guid)      
        self.logger.info(msg)
        print(msg)
        
        job.processed = 1
        job.processeddate = datetime.now()
        job.save()
              
        self.logger.debug('Job marked as processed and saved.')

        #SEND THE DOWNLOAD LINK
                
        #set the subject and from address from settings file
        subject, from_email = \
            settings.download['subject'], settings.download['fromemail']

        #get the plain text template
        plaintext = django.template.loader.get_template(settings.download['templateplaintext'])

        #replace the placeholders in the template
        d = django.template.Context({
            'downloadlink': settings.download['linkurl'] + '?uid=' + job.guid
        })

        #only plaintext for now
        text_content = plaintext.render(d)

        #create the message
        msg = EmailMultiAlternatives(subject, text_content, from_email, [job.email])

        #send the message
        try:
            msg.send()
        except: #download link was not sent successfully
            print 'Download link email for %s failed to send.' % job.guid

        
            
    def importdata(self, job):    
        '''importdata: imports file described in Filestore object to database'''
        print '''job in queue is dataset with id: %s.
    Uploaded on %s by %s
        ''' % (job.guid, job.creationdate, job.email)
        
        filepath = settings.FILESAVEPATH + job.filename
        # verify the file still exists and get its columns
        try:
            columns = file( 
                settings.FILESAVEPATH + job.filename 
            ).readline().rstrip('\n').split(',')
        except Filestore.DoesNotExist:
            # blitz from queue
            job.delete()
            raise CommandError(
                'Upload with reference "%s" does not exist' % job.filename
            )
        # Wrap columns in double quotes if not present
        columns = ['"'+column.strip('"')+'"' for column in columns]

        
        # Open connection to PostgreSQL
        connstr = "dbname=%s user=%s password=%s" % \
        ( 
            settings.postgresql['dbname'], \
            settings.postgresql['user'], \
            settings.postgresql['password']
        )
        self.logger.debug('Opening database connection: ' + connstr)
        dbconn = conn = psycopg2.connect(connstr)
        cur = dbconn.cursor()        
        self.logger.debug('Database connection opened.')

        tablename = 'dataset_'+job.guid+'_import'
        tempname = tablename+"_temp"

        # Copy data into temporary table
        sql = "create temp table %s (%s text);" % \
        (tempname, " text, ".join(columns))
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)
        
        # Should perhaps use psycopg2 copy_from method
        sql = "copy %s (%s) from '%s' with CSV header;" % \
        (
            tempname, 
            ", ".join(columns), 
            filepath
        )
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)
        
        # Move data into actual table, forcing correct types      
        sql = "drop table if exists %s;\n" % (tablename)
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)
        
        sql = "create table %s as ( \
select %s::integer as easting, %s::integer as northing, %s::date as date \
from %s where %s::integer>-1 and %s::integer>-1 and to_char(to_date(%s,'YYYY-MM-DD'),'YYYY-MM-DD') = %s );\n" % \
        (
            tablename,
            next((n for n in columns if n.lower() == '"easting"'), 'EastingColumn'),
            next((n for n in columns if n.lower() == '"northing"'), 'NorthingColumn'),
            next((n for n in columns if n.lower() == '"date"'), 'DateColumn'), 
            tempname,
            next((n for n in columns if n.lower() == '"easting"'), 'EastingColumn'),
            next((n for n in columns if n.lower() == '"northing"'), 'NorthingColumn'),
            next((n for n in columns if n.lower() == '"date"'), 'DateColumn'), 
            next((n for n in columns if n.lower() == '"date"'), 'DateColumn')
        )
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)

        # Drop temp table
        sql = "drop table if exists %s;\n" % (tempname)        
        self.logger.debug('SQL: ' + sql)
        cur.execute(sql)

        # Commit changes to database
        self.logger.debug('Committing changes.')
        dbconn.commit()
        self.logger.debug('Changes committed.')

        # Mark job as imported
       
        job.processed = -1
        job.save()
        self.logger.debug('Job marked as imported and saved.')
        

        # Close database connection
        cur.close()
        dbconn.close()   
        self.logger.debug('Database connection closed')     
        
        msg='Successfully imported data from "%s"\n' % job.filename
        self.logger.info(msg)
        print(msg)

