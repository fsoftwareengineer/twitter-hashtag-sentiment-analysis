# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from stream_processor import StreamProcessor
import json

__stream_name__ = 'hashtag-twitter'
__s3_bucket_name__ = 'stream-bucket'
# Variables that contains the user credentials to access Twitter API
access_token = "<ACCESS_TOKEN>"
access_token_secret = "<ACCESS_TOKEN_SECRET>"
consumer_key = "<CONSUMER_KEY>"
consumer_secret = "<CONSUMER_SECRET>"


# This is a basic listener that just prints received tweets to stdout.
class Listener(StreamListener):
    stream_processor = None

    def __init__(self, stream_processor):
        super().__init__()
        self.stream_processor = stream_processor

    def on_data(self, data):
        json_data = json.loads(data)
        print(json_data)
        response = self.stream_processor.put_record(json_data['text'])
        print(str(response))
        return True

    def on_error(self, status):
        print("ERROR: " + str(status))


def start_stream(lan, hashtag):
    # This handles Twitter authentication and the connection to Twitter Streaming API
    processor = StreamProcessor(stream_name=__stream_name__, storage_name=__s3_bucket_name__)
    processor.create_processor()

    l = Listener(stream_processor=processor)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    print("Start filtering... language : " + lan + " hashtag : " + hashtag)
    stream.filter(languages=[lan], track=[hashtag])
