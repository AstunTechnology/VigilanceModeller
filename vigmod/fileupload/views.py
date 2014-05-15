# Create your views here.
from django.template import Context, loader
from django.shortcuts import render_to_response
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.core.mail import EmailMultiAlternatives
from django.conf import settings
from django.template.loader import get_template
import hashlib
import random
import csv
import os
import string
from datetime import datetime
#custom settings for the fileupload app
from vigmod.fileupload.conf import settings
from vigmod.fileupload.models import Filestore
from vigmod.fileupload.models import UploadFileForm


def upload(request):
    def errorHandle(error):
	form = UploadFileForm()
	return render_to_response('upload.html', {
        'error' : error,
		'form' : form,
	})
    if request.method == 'POST':
        #get the form details
        form = UploadFileForm(request.POST, request.FILES)
        #if the form is valid then continue
        if form.is_valid():
            #check for a valid file extension
            try:
                filename = request.FILES['file'].name
                if os.path.splitext(filename)[1] != '.csv':
                    return render_to_response('upload.html', {'form': form, 'error': settings.error['invalidextension']})
            except:
                return render_to_response('upload.html', {'form': form, 'error': settings.error['extensionerror']})    
           
            #create a guid for the database and email verification link
            guid = hashlib.sha1(str(random.random())).hexdigest()

            #give the file a unique name
            filename = 'import_data_' + guid + '.csv';

            #temporarily store the file until the email is validated
            try:
                handle_uploaded_file(request.FILES['file'], filename)
            except:
                #Return error to the page
                return render_to_response('upload.html', {'form': form, 'error': settings.error['filesaveerror']})

            #check if the csv file is in the correct format
            ifile  = open(settings.FILESAVEPATH + filename, "rb")
            blInvalidDate = False
            error = ''

            #read the csv file
            reader = csv.reader(ifile)

            rownum = 0
            for row in reader:
                # check the header columns
                if rownum == 0:
                    header = row
                    if len(header) != 3:
                        error += settings.error['columncountwrong']
                    try:                    
                        if string.lower(header[0]) != 'date':
                            error += settings.error['nodatecolumn']

                        if string.lower(header[1]) != 'easting':
                            error += settings.error['noeastingcolumn']

                        if string.lower(header[2]) != 'northing':
                            error += settings.error['nonorthingcolumn']
                    except:
                        #Add Error
                        error += settings.error['invalidheader']

                #if no error then check the dates
                if error == '':
                    if (rownum >= 1):
                        #check if the date is in the correct format
                        colnum = 0

                        for col in row:
                            #date field
                            if (colnum == 0):
                                try:
                                    d = datetime.strptime(col, '%Y-%m-%d')
                                    formatteddate = d.strftime('%Y-%m-%d')
                                
                                    if formatteddate != col:
                                        blInvalidDate = True
                                    colnum += 1
                                except:
                                    blInvalidDate = True

                            if blInvalidDate:
                                error = settings.error['invaliddate']

                #increment the row num
                rownum += 1
                       
            ifile.close()

            #if the file is in the correct format then continue
            if error == '':
                email = request.POST['email']
                               
                #STORE THE INFO IN THE DATABASE

                #set the values        
                dbrecord = Filestore(
                    guid = guid,
                    creationdate = datetime.now(),
                    verificationdate = datetime.now(),
                    filename = filename,
                    email = email,
                    emailsent = 0,
                    emailverified = 0,
                    filepath = settings.FILESAVEPATH,
                    processed = 0
                )

                #save to the db
                try:
                    dbrecord.save()
                except:
                    #Send error to page
                    return render_to_response('upload.html', {'form': form, 'error': settings.error['dbsaveerror']})
                
                #SEND THE VERIFICATION LINK
                
                #set the subject and from address from settings file
                subject, from_email = settings.verification['verificationsubject'], settings.verification['verificationfromemail']

                #get the plain text template
                plaintext = loader.get_template(settings.verification['templateplaintext'])

                #replace the placeholders in the template
                d = Context({
                    'verificationlink': settings.verification['linkurl'] + '?uid=' + guid
                })

                #only plaintext for now
                text_content = plaintext.render(d)

                #create the message
                msg = EmailMultiAlternatives(subject, text_content, from_email, [email])
                #if we want to attach the html message
                #msg.attach_alternative(html_content, "text/html")

                #send the message
                try:
                    msg.send()

                    #IF THE LINK WAS SENT SUCCESSFULLY THEN UPDATE THE TABLE
                    
                    #get the record from the database
                    dbrecord = Filestore.objects.get(guid=guid)

                    #if found then update the emailsent field
                    dbrecord.emailsent = 1
                    dbrecord.save()

                    #redirect to the success page               
                    return HttpResponseRedirect('/fileupload/success')
                except: #verification link was not sent successfully
                    #Add error message to the page
                    return render_to_response('upload.html', {'form': form, 'error': settings.error['emailerror']})
            else: #Return the error
                #Remove the file
                os.remove(settings.FILESAVEPATH + filename)
                
                return render_to_response('upload.html', {
                'form': form, 'error': error,
            })
        else: #Return the form with validation hints
            return render_to_response('upload.html', {
                'form': form,
            })
    else: #Return a blank form
        form = UploadFileForm()
        return render_to_response('upload.html', {
            'form': form,
        })

#call this page when the user validated his email address
def validatelink(request):
    guid = request.GET.get('uid', '')

    try:    
        #get the record from the database
        dbrecord = Filestore.objects.get(guid=guid)

        #if already verified
        if dbrecord.emailverified == 1:
            return HttpResponse('email already verified.')
        else:
            #if found then update the link to be validated
            dbrecord.emailverified = 1
            dbrecord.verificationdate = datetime.now()
            dbrecord.save()

            return render_to_response('validatelink.html') 
    except:
        return HttpResponse(settings.error['norecord'])
               
#Page to call when file is uploaded successfully
def success(request):
    t = loader.get_template('success.html')
    c = Context({})
    return HttpResponse(t.render(c))  
       
#Page for downloading of generated files
def download(request):
    guid = request.GET.get('uid', '')    
    try:    
        #get the record from the database
        dbrecord = Filestore.objects.get(guid=guid)
        return render_to_response(
            'download.html', 
            {
                'mapinfofile':guid+'_mapinfo.zip',
                'shapefile':guid+'_shapefile.zip',
                #'csvfile':guid+'_csv.zip',
            }
        ) 
    except:
        return HttpResponse(settings.error['norecord'])   

#Method to save the file to a directory as specified in settings
def handle_uploaded_file(f, filename):
    destination = open(settings.FILESAVEPATH + filename, 'wb+')
    for chunk in f.chunks():
        destination.write(chunk)
    destination.close()


    




