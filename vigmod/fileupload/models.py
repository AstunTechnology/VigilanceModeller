from django.db import models
from django import forms
from django.core.validators import validate_email

# Create your models here.
class Filestore(models.Model):
    guid = models.CharField(max_length=254,primary_key = True)
    creationdate = models.DateTimeField()
    verificationdate = models.DateTimeField()
    processeddate = models.DateTimeField(null=True)
    email = models.CharField(max_length=254)
    filename = models.CharField(max_length=254)
    filepath = models.CharField(max_length=254)
    emailsent = models.IntegerField()
    emailverified = models.IntegerField()
    processed = models.IntegerField()

class UploadFileForm(forms.Form):
    email = forms.EmailField()
    file = forms.FileField()

   
    




    

    
    

