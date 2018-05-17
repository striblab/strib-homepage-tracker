import json, os, datetime, csv, boto

# Set these in your environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET', '')

########## FUNCTIONS ##########

def _download_files(path):
    print 'Downloading files ...'
    if not os.path.exists('./data'):
        os.makedirs('./data')

    conn  = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    bucket = conn.get_bucket(AWS_S3_BUCKET)

    for file in bucket.list(path):
        localpath = './data/%s' % file.key.split('/')[-1]
        if not os.path.exists(localpath) and localpath.endswith('json'):
            print 'Getting %s' % file.key
            file.get_contents_to_filename(localpath)
    print 'Done!'
    return

def _get_files():
    for filename in os.listdir('./data/'):
        if filename.endswith('json'):
            yield open('./data/%s' % filename, 'r').read()

########## MAIN ##########

if __name__ == '__main__':
    _download_files('projects/homepage-tracker/data/')

    print 'Formatting csv ...'
    with open('hp_stories.csv', 'wb') as csvfile:
        hpwriter = csv.writer(csvfile, delimiter=',', quotechar='"')
        hpwriter.writerow(['story_id', 'headline', 'position', 'datetime'])

        for f in _get_files():
            j = json.loads(f)

            for story in j:
                if story.get('type') == 'story':
                    headline = story.get('headline').encode('utf-8')
                    position = story.get('position')
                    dt = datetime.datetime.strptime(story.get('datetime'), '%Y-%m-%d %H:%M:%S')
                    story_id = story.get('id')

                    hpwriter.writerow([story_id, headline, position, dt])
                elif story.get('type') == 'top_headlines':
                    for th in story.get('contents'):
                        headline = th.get('headline').encode('utf-8')
                        position = th.get('position')
                        dt = datetime.datetime.strptime(th.get('datetime'), '%Y-%m-%d %H:%M:%S')
                        story_id = th.get('id')

                        hpwriter.writerow([story_id, headline, position, dt])
    print 'Done!'
