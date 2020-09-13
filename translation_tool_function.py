import boto3 # standard library to integrate Python with AWS
import uuid  # generates a random unique string
import os    # allows you to use command line tools
import json  # loads the .json file from S3
from contextlib import closing  # used for converting type AudioStream to a readable format
from tempfile import gettempdir # creates a temporary directory in the OS
import time  # adds delays in the code

def lambda_handler(event, context):
    translate = boto3.client('translate')
    transcribe = boto3.client('transcribe')
    polly = boto3.client('polly')
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    bucket = event['Records'][0]['s3']['bucket']['name'] # the name of the bucket that was added to
    file = event['Records'][0]['s3']['object']['key']    # the name of the object that was added
    
    output_bucket = "output-23784934"
    job_name = str(uuid.uuid4())  #makes a random unique alphanumeric string
    
    transcribe_response = transcribe.start_transcription_job(
        TranscriptionJobName= job_name,
        LanguageCode='en-US',
        MediaFormat='mp3',
        Media={
            'MediaFileUri': 's3://' + bucket + "/" + file
        },
        OutputBucketName=output_bucket
    )
    
    found = True
    while (found):
        try:
            object = s3_resource.Object(output_bucket, job_name+'.json') # a reference to the S3 object
            break
        except Exception as e:
            time.sleep(0.1)
    
    BUCKET = output_bucket
    FILE_TO_READ = job_name + ".json"
    
    while (found):
        try:
            result = s3.get_object(Bucket=BUCKET, Key=FILE_TO_READ) # retrieves the S3 object
            text = result["Body"].read().decode()
            text = json.loads(text) # loads the JSON file
            translation = text["results"]["transcripts"][0]["transcript"] # extracts the transcript data
            break
        except Exception as e:
            time.sleep(0.1)
    
    translate_response = translate.translate_text(
        Text=translation,
        SourceLanguageCode='en',
        TargetLanguageCode='es'
    )
    
    polly_response = polly.synthesize_speech(
        LanguageCode = 'es-MX',
        OutputFormat='mp3',
        Text = translate_response['TranslatedText'],
        VoiceId = 'Camila'
    )
    
    if "AudioStream" in polly_response:
        with closing(polly_response["AudioStream"]) as stream:
            output = os.path.join(gettempdir(), job_name + ".mp3")
            try:
                with open(output, "wb") as file:
                    file.write(stream.read())
            except IOError as error:
                time.sleep(0.1)
    response = s3.upload_file(output, "media-files2348923", job_name+".mp3")
    
    delete_bucket = s3_resource.Bucket(output_bucket)
    delete_bucket.objects.all().delete()
    
    return {
        'statusCode': 200,
    }
