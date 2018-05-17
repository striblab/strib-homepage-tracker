import os, json, requests, datetime, boto
from bs4 import BeautifulSoup
from boto.s3.key import Key

# Set these in your environment variables
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET', '')

NOW = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

########## FUNCTIONS ##########

def relateds_to_json(obj):
    '''
    Captures related articles to a story.
    '''
    relateds = []
    for a in obj:
        relateds.append({
            'datetime': NOW,
            'id': a['href'],
            'headline': a['data-linkname'],
            'module': a['data-moduletype'],
            'position': a['data-position']
        })
    return relateds

def story_to_json(obj):
    '''
    Formats a story represented in one of the 12 manually curated content
    wells as JSON.
    '''
    relateds = obj.find_all('a', {'class': 'tease-list-item-link'})
    json_obj = {
        'type': 'story',
        'datetime': NOW,
        'id': obj.h3.a['href'],
        'headline': obj.h3.a['data-linkname'],
        'module': obj.h3.a['data-moduletype'],
        'position': obj.h3.a['data-position'],
        'html_raw': unicode(obj),
        'relateds': relateds_to_json(relateds) if relateds else []
    }
    return json_obj

def top_headlines_to_json(obj):
    '''
    Creates JSON object out of the "More top headlines" section.
    '''
    top_headlines = {'type': 'top_headlines', 'html_raw': unicode(obj), 'contents': []}
    for story in obj.find_all('div', {'class': 'collection-story'}):
        top_headlines['contents'].append({
            'datetime': NOW,
            'id': story.h4.a['href'],
            'headline': story.h4.a['data-linkname'],
            'module': story.h4.a['data-moduletype'],
            'position': story.h4.a['data-position']
        })
    return top_headlines

def save_local(obj, path='./data', filename="hp%s.json" % datetime.datetime.now().strftime('%Y%m%d%H%M%S')):
    '''
    Save the JSON object to a local directory.
    '''
    with open(os.path.join(path, filename), 'w') as f:
        f.write(json.dumps(obj))

def save_to_s3(obj, path='projects/homepage-tracker/data/', filename="hp%s.json" % datetime.datetime.now().strftime('%Y%m%d%H%M%S'), headers={}):
    try:
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        bucket_obj = conn.get_bucket(AWS_S3_BUCKET)
    except:
        print 'Problem connecting to S3'
        raise

    key = Key(bucket_obj, os.path.join(path, filename))
    key.set_contents_from_string(json.dumps(obj), headers)
    return

########## MAIN ##########

if __name__ == '__main__':
    # Get the Star Tribune homepage
    page = requests.get('http://www.startribune.com').content
    soup = BeautifulSoup(page, 'html.parser')

    # Get all the teases, which correspond to the manually curated wells
    teases = soup.find_all('div', {'class': 'tease'})

    # Quick-and-dirty way to transform those teases to JSON
    hp_list = []
    for tease in teases:
        is_collection = True if 'collection-tease' in tease['class'] else False
        if is_collection:
            if 'left-well-default-6' in tease['class']:
                hp_list.append(top_headlines_to_json(tease))
        else:
            hp_list.append(story_to_json(tease))

    save_to_s3(hp_list)
