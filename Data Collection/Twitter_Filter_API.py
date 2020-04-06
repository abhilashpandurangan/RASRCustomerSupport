import pandas as pd
import time
import tweepy as tw
class MyStreamListener(tw.StreamListener):

    def __init__(self):
        super(MyStreamListener,self).__init__()
        self.tweetText = []
        self.tweetScreenName = []
        self.tweetId = []
        self.replyName = []
        self.replyId = []
        self.count = 0
    def get_tweet_text(self,tweet):
        try:
            return tweet.extended_tweet['full_text']
        except AttributeError as e:
            return tweet.text
    def on_status(self, status):
        self.tweetText.append(self.get_tweet_text(status))
        self.tweetScreenName.append(status.user.screen_name)
        self.tweetId.append(status.id_str)
        self.replyName.append(status.in_reply_to_screen_name)
        self.replyId.append(status.in_reply_to_status_id)
        self.count += 1
        print(self.get_tweet_text(status))
        if self.count == 100:
            dataTemp=pd.DataFrame();
            dataTemp['TweetScreenName']=self.tweetScreenName
            dataTemp['TweetId']=self.tweetId
            dataTemp['UserName'] = self.replyName
            dataTemp['UserID'] = self.replyId
            dataTemp['TweetText']=self.tweetText
            timestr = time.strftime("%Y%m%d-%H%M%S")
            dataTemp.to_csv(timestr+' '+str(self.count)+'.csv',index=False)
            self.tweetText = []
            self.tweetScreenName = []
            self.tweetId = []
            self.replyName = []
            self.replyId = []
            self.count = 0
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
        # returning non-False reconnects the stream, with backoff.

def start_stream(stream, **kwargs):
        try:
		    #Language used - Hindi
            stream.filter(**kwargs, languages = ['hi'])
        except Exception:
            print('except')

#KEYS HERE
consumer_key= ''
consumer_secret= ''
access_token= ''
access_token_secret= ''

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

realTimeTweets = []
myStreamListener = MyStreamListener()
myStream = tw.Stream(auth = api.auth, listener=myStreamListener, tweet_mode='extended')

#List of Customer Support handles (Food Delivery, Payment Applications, Airlines)
follow_list = {'@paytmcare': '2475273985','zomato' : "23283603", 'swiggy_in' :  "2639625036", 'UberINSupport' : "794125504357900289", 'olasupports' : "2542889478", 'swiggycares': "3286336254"}

start_stream(myStream,follow = list(follow_list.values()) ,is_async=True)